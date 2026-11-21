"""Clip backend: accounts, videos, likes, follows, live streams, gifts, wallet.
Python 3.9+ standard library only. Run: python server.py  (http://localhost:8000)
Live chat and gifts are pushed to viewers with Server-Sent Events (SSE)."""
import hashlib, json, os, queue, re, secrets, sqlite3, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", 8000))
MOCK_PAYMENTS = os.environ.get("CLIP_MOCK_PAYMENTS", "1") == "1"  # set 0 in production
DIAMOND_PER_COIN = 1.0       # diamonds the host earns per coin gifted
DIAMONDS_PER_CENT = 2        # payout: 2 diamonds = 1 cent (whole cents, rounded down)
GIFTS = {"rose": 1, "coffee": 5, "crown": 50, "lion": 299, "rocket": 500}
PACKS = {70: 99, 350: 499, 700: 999}  # coins -> price in cents

DB = sqlite3.connect(os.environ.get("CLIP_DB", "clip.db"), check_same_thread=False, isolation_level=None)
DB.row_factory = sqlite3.Row
LOCK = threading.RLock()
DB.executescript("""
PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL, pw TEXT NOT NULL,
  coins INTEGER NOT NULL DEFAULT 100 CHECK(coins>=0), diamonds INTEGER NOT NULL DEFAULT 0 CHECK(diamonds>=0));
CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id));
CREATE TABLE IF NOT EXISTS videos(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id),
  caption TEXT NOT NULL, emoji TEXT NOT NULL, bg INTEGER NOT NULL, created REAL NOT NULL);
CREATE TABLE IF NOT EXISTS likes(user_id INTEGER, video_id INTEGER, PRIMARY KEY(user_id, video_id));
CREATE TABLE IF NOT EXISTS follows(follower_id INTEGER, followee_id INTEGER, PRIMARY KEY(follower_id, followee_id));
CREATE TABLE IF NOT EXISTS lives(id INTEGER PRIMARY KEY, host_id INTEGER NOT NULL REFERENCES users(id),
  started REAL NOT NULL, ended REAL);
CREATE TABLE IF NOT EXISTS gifts(id INTEGER PRIMARY KEY, live_id INTEGER, from_id INTEGER, gift TEXT, coins INTEGER, created REAL);
CREATE TABLE IF NOT EXISTS purchases(id INTEGER PRIMARY KEY, user_id INTEGER, coins INTEGER, cents INTEGER, created REAL);
""")

def rows(sql, a=()):
    with LOCK: return [dict(r) for r in DB.execute(sql, a).fetchall()]
def one(sql, a=()):
    r = rows(sql, a); return r[0] if r else None

class Err(Exception):
    def __init__(s, code, msg): s.code, s.msg = code, msg

def tx(fn):
    """Run fn() inside one atomic transaction."""
    with LOCK:
        DB.execute("BEGIN IMMEDIATE")
        try: out = fn(); DB.execute("COMMIT"); return out
        except BaseException: DB.execute("ROLLBACK"); raise

def hash_pw(pw, salt=None):
    salt = salt or secrets.token_hex(16)
    return salt + "$" + hashlib.scrypt(pw.encode(), salt=salt.encode(), n=2**14, r=8, p=1).hex()
def check_pw(pw, stored):
    salt = stored.split("$")[0]
    return secrets.compare_digest(hash_pw(pw, salt), stored)

# ---- live rooms: live_id -> list of subscriber queues (one per viewer) ----
ROOMS, RLOCK = {}, threading.Lock()
def publish(live_id, event, data):
    with RLOCK:
        for q in ROOMS.get(live_id, []): q.put((event, data))
def viewers(live_id):
    with RLOCK: return len(ROOMS.get(live_id, []))

def new_session(uid):
    t = secrets.token_urlsafe(32)
    with LOCK: DB.execute("INSERT INTO sessions VALUES(?,?)", (t, uid))
    return t

def video_json(v, me):
    v = dict(v)
    v["liked"] = bool(me and one("SELECT 1 x FROM likes WHERE user_id=? AND video_id=?", (me["id"], v["id"])))
    v["following"] = bool(me and one("SELECT 1 x FROM follows f JOIN users u ON u.id=f.followee_id WHERE f.follower_id=? AND u.name=?", (me["id"], v["by"])))
    return v

VIDEO_SQL = """SELECT v.id, u.name AS by, v.caption, v.emoji, v.bg,
  (SELECT COUNT(*) FROM likes l WHERE l.video_id=v.id) AS likes FROM videos v JOIN users u ON u.id=v.user_id"""

# ---- handlers: each takes (me, body, *path_groups) and returns JSON-able data ----
def signup(me, b):
    name, pw = str(b.get("username", "")).strip().lower(), str(b.get("password", ""))
    if not re.fullmatch(r"[a-z0-9_]{3,20}", name): raise Err(400, "Username: 3-20 letters, numbers or _")
    if len(pw) < 8: raise Err(400, "Password must be at least 8 characters")
    try:
        with LOCK: uid = DB.execute("INSERT INTO users(name,pw) VALUES(?,?)", (name, hash_pw(pw))).lastrowid
    except sqlite3.IntegrityError: raise Err(409, "Username taken")
    return {"token": new_session(uid), "username": name}

def login(me, b):
    u = one("SELECT * FROM users WHERE name=?", (str(b.get("username", "")).lower(),))
    if not u or not check_pw(str(b.get("password", "")), u["pw"]): raise Err(401, "Wrong username or password")
    return {"token": new_session(u["id"]), "username": u["name"]}

def profile(me, b, name):
    u = one("SELECT id,name FROM users WHERE name=?", (name,))
    if not u: raise Err(404, "No such user")
    r = {"username": u["name"],
         "followers": one("SELECT COUNT(*) c FROM follows WHERE followee_id=?", (u["id"],))["c"],
         "following": one("SELECT COUNT(*) c FROM follows WHERE follower_id=?", (u["id"],))["c"],
         "likes": one("SELECT COUNT(*) c FROM likes l JOIN videos v ON v.id=l.video_id WHERE v.user_id=?", (u["id"],))["c"],
         "videos": [video_json(v, me) for v in rows(VIDEO_SQL + " WHERE v.user_id=? ORDER BY v.id DESC", (u["id"],))]}
    if me and me["id"] == u["id"]: r.update(coins=me["coins"], diamonds=me["diamonds"])
    if me: r["i_follow"] = bool(one("SELECT 1 x FROM follows WHERE follower_id=? AND followee_id=?", (me["id"], u["id"])))
    return r

def my_profile(me, b): return profile(me, b, me["name"])
def feed(me, b): return [video_json(v, me) for v in rows(VIDEO_SQL + " ORDER BY v.id DESC LIMIT 50")]

def post_video(me, b):
    cap = str(b.get("caption", "")).strip()[:60]
    if not cap: raise Err(400, "Caption required")
    with LOCK:
        vid = DB.execute("INSERT INTO videos(user_id,caption,emoji,bg,created) VALUES(?,?,?,?,?)",
                         (me["id"], cap, str(b.get("emoji", "🎬"))[:8], int(b.get("bg", 0)) % 4, time.time())).lastrowid
    return one(VIDEO_SQL + " WHERE v.id=?", (vid,))

def toggle(table, cols, vals):
    def f():
        if DB.execute(f"DELETE FROM {table} WHERE {cols[0]}=? AND {cols[1]}=?", vals).rowcount: return False
        DB.execute(f"INSERT INTO {table} VALUES(?,?)", vals); return True
    return tx(f)

def like(me, b, vid):
    if not one("SELECT 1 x FROM videos WHERE id=?", (vid,)): raise Err(404, "No such video")
    on = toggle("likes", ("user_id", "video_id"), (me["id"], int(vid)))
    return {"liked": on, "likes": one("SELECT COUNT(*) c FROM likes WHERE video_id=?", (vid,))["c"]}

def follow(me, b, name):
    u = one("SELECT id FROM users WHERE name=?", (name,))
    if not u: raise Err(404, "No such user")
    if u["id"] == me["id"]: raise Err(400, "You can't follow yourself")
    on = toggle("follows", ("follower_id", "followee_id"), (me["id"], u["id"]))
    return {"following": on, "followers": one("SELECT COUNT(*) c FROM follows WHERE followee_id=?", (u["id"],))["c"]}

def live_start(me, b):
    if one("SELECT 1 x FROM lives WHERE host_id=? AND ended IS NULL", (me["id"],)): raise Err(409, "You are already live")
    with LOCK: lid = DB.execute("INSERT INTO lives(host_id,started) VALUES(?,?)", (me["id"], time.time())).lastrowid
    return {"live_id": lid}

def live_list(me, b):
    return [{"live_id": r["id"], "host": r["host"], "viewers": viewers(r["id"])} for r in
            rows("SELECT l.id, u.name host FROM lives l JOIN users u ON u.id=l.host_id WHERE l.ended IS NULL")]

def get_live(lid):
    l = one("SELECT * FROM lives WHERE id=? AND ended IS NULL", (lid,))
    if not l: raise Err(404, "Stream not found or ended")
    return l

def live_end(me, b, lid):
    l = get_live(lid)
    if l["host_id"] != me["id"]: raise Err(403, "Only the host can end the stream")
    with LOCK: DB.execute("UPDATE lives SET ended=? WHERE id=?", (time.time(), l["id"]))
    earned = one("SELECT COALESCE(SUM(coins),0) c FROM gifts WHERE live_id=?", (l["id"],))["c"]
    publish(l["id"], "end", {"earned_diamonds": int(earned * DIAMOND_PER_COIN)})
    return {"earned_diamonds": int(earned * DIAMOND_PER_COIN)}

def live_chat(me, b, lid):
    get_live(lid); t = str(b.get("text", "")).strip()[:140]
    if not t: raise Err(400, "Empty message")
    publish(int(lid), "chat", {"user": me["name"], "text": t}); return {"ok": True}

def live_gift(me, b, lid):
    l = get_live(lid); g = str(b.get("gift", "")).lower()
    if g not in GIFTS: raise Err(400, "Unknown gift")
    if l["host_id"] == me["id"]: raise Err(400, "You can't gift your own stream")
    cost, earn = GIFTS[g], int(GIFTS[g] * DIAMOND_PER_COIN)
    def f():  # atomic: charge sender, credit host, record
        if not DB.execute("UPDATE users SET coins=coins-? WHERE id=? AND coins>=?", (cost, me["id"], cost)).rowcount:
            raise Err(402, "Not enough coins")
        DB.execute("UPDATE users SET diamonds=diamonds+? WHERE id=?", (earn, l["host_id"]))
        DB.execute("INSERT INTO gifts(live_id,from_id,gift,coins,created) VALUES(?,?,?,?,?)", (l["id"], me["id"], g, cost, time.time()))
    tx(f)
    publish(l["id"], "gift", {"user": me["name"], "gift": g, "coins": cost})
    return {"coins": one("SELECT coins FROM users WHERE id=?", (me["id"],))["coins"]}

def buy(me, b):
    coins = int(b.get("coins", 0))
    if coins not in PACKS: raise Err(400, "Unknown coin pack")
    if not MOCK_PAYMENTS:  # production: credit coins only from a verified payment-provider webhook
        raise Err(501, "Connect a payment provider (e.g. Stripe Checkout + webhook)")
    def f():
        DB.execute("UPDATE users SET coins=coins+? WHERE id=?", (coins, me["id"]))
        DB.execute("INSERT INTO purchases(user_id,coins,cents,created) VALUES(?,?,?,?)", (me["id"], coins, PACKS[coins], time.time()))
    tx(f); return {"coins": one("SELECT coins FROM users WHERE id=?", (me["id"],))["coins"]}

def cashout(me, b):
    def f():
        d = DB.execute("SELECT diamonds FROM users WHERE id=?", (me["id"],)).fetchone()[0]
        if d < 1: raise Err(400, "No diamonds to cash out")
        DB.execute("UPDATE users SET diamonds=0 WHERE id=?", (me["id"],)); return d
    d = tx(f)  # production: send these cents via a payout provider after KYC checks
    return {"diamonds": d, "usd": (d // DIAMONDS_PER_CENT) / 100}

ROUTES = [  # (method, regex, handler, needs_auth)
    ("POST", r"/api/signup", signup, False), ("POST", r"/api/login", login, False),
    ("GET", r"/api/me", my_profile, True), ("GET", r"/api/users/([a-z0-9_]+)", profile, False),
    ("GET", r"/api/feed", feed, False), ("POST", r"/api/videos", post_video, True),
    ("POST", r"/api/videos/(\d+)/like", like, True), ("POST", r"/api/users/([a-z0-9_]+)/follow", follow, True),
    ("GET", r"/api/live", live_list, False), ("POST", r"/api/live/start", live_start, True),
    ("POST", r"/api/live/(\d+)/end", live_end, True), ("POST", r"/api/live/(\d+)/chat", live_chat, True),
    ("POST", r"/api/live/(\d+)/gift", live_gift, True),
    ("POST", r"/api/wallet/buy", buy, True), ("POST", r"/api/wallet/cashout", cashout, True),
]

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def send_json(self, code, obj):
        d = json.dumps(obj).encode()
        self.send_response(code)
        for k, v in (("Content-Type", "application/json"), ("Content-Length", len(d)), ("Access-Control-Allow-Origin", "*"),
                     ("Access-Control-Allow-Headers", "Authorization, Content-Type")): self.send_header(k, v)
        self.end_headers(); self.wfile.write(d)
    def user(self):
        t = self.headers.get("Authorization", "")[7:] or self.path.partition("token=")[2]
        return one("SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token=?", (t,)) if t else None
    def do_OPTIONS(self): self.send_json(204, {})
    def do_GET(self): self.route("GET")
    def do_POST(self): self.route("POST")
    def route(self, method):
        path = self.path.split("?")[0]
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
            if method == "GET" and path in ("/", "/index.html"):
                page = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html"), "rb").read()
                self.send_response(200)
                for k, v in (("Content-Type", "text/html; charset=utf-8"), ("Content-Length", len(page))): self.send_header(k, v)
                self.end_headers(); return self.wfile.write(page)
            m = re.fullmatch(r"/api/live/(\d+)/stream", path)
            if method == "GET" and m: return self.stream(m.group(1))
            for meth, rx, fn, auth in ROUTES:
                mm = re.fullmatch(rx, path)
                if meth == method and mm:
                    me = self.user()
                    if auth and not me: raise Err(401, "Log in first")
                    return self.send_json(200, fn(me, body, *mm.groups()))
            raise Err(404, "Not found")
        except Err as e: self.send_json(e.code, {"error": e.msg})
        except (ValueError, TypeError): self.send_json(400, {"error": "Bad request"})
        except Exception as e: print("ERROR", repr(e)); self.send_json(500, {"error": "Server error"})
    def stream(self, lid):
        """SSE: viewers subscribe here and receive chat, gift, viewers and end events."""
        l = get_live(lid); lid = int(lid); q = queue.Queue()
        with RLOCK: ROOMS.setdefault(lid, []).append(q)
        self.send_response(200)
        for k, v in (("Content-Type", "text/event-stream"), ("Cache-Control", "no-cache"), ("Access-Control-Allow-Origin", "*")): self.send_header(k, v)
        self.end_headers()
        try:
            publish(lid, "viewers", {"count": viewers(lid)})
            while True:
                try: ev, data = q.get(timeout=15); msg = f"event: {ev}\ndata: {json.dumps(data)}\n\n"
                except queue.Empty: msg = ": keepalive\n\n"
                self.wfile.write(msg.encode()); self.wfile.flush()
                if msg.startswith("event: end"): break
        except (BrokenPipeError, ConnectionResetError, OSError): pass
        finally:
            with RLOCK: ROOMS[lid].remove(q)
            publish(lid, "viewers", {"count": viewers(lid)}); self.close_connection = True

if __name__ == "__main__":
    ThreadingHTTPServer.daemon_threads = True
    print(f"Clip backend on http://localhost:{PORT}")
    ThreadingHTTPServer(("", PORT), H).serve_forever()
