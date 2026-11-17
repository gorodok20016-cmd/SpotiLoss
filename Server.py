#!/usr/bin/env python3
"""
Spotiloss — сервер (новая версия).

  • поиск:   SpotAPI  ->  запасные: Deezer  ->  iTunes  (если один сломался, работает следующий)
  • скачать: spotDL (треки кладутся в папку downloads/, повторно не качаются)
  • сам отдаёт spotiloss.html: просто открой  http://localhost:8000  в браузере

Установка:   pip install spotdl spotapi        (spotapi необязателен)
             ffmpeg:  Termux — pkg install ffmpeg,  Windows/Mac — установи ffmpeg
Запуск:      python server.py                  (или  python server.py --port 9000)

Только для личного использования. Соблюдай правила сервисов и авторские права.
"""
import argparse, concurrent.futures as cf, hashlib, json, re, shutil, subprocess, sys
import threading, time, urllib.parse, urllib.request, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
AUDIO = (".mp3", ".m4a", ".opus", ".ogg", ".flac", ".wav")
MIME = {".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".opus": "audio/ogg", ".ogg": "audio/ogg",
        ".flac": "audio/flac", ".wav": "audio/wav", ".html": "text/html; charset=utf-8"}
JOBS, LOCK = {}, threading.Lock()
SLOTS = threading.Semaphore(2)          # одновременно качаем не больше 2 треков
POOL = cf.ThreadPoolExecutor(max_workers=4)
ROOT = HERE / "downloads"
MAX_MB = 2000                           # когда папка больше — старые треки удаляются


# ───────────────────────── проверка окружения ─────────────────────────
def have_module(name):
    try:
        __import__(name)
        return True
    except Exception:
        return False


def health():
    return {"spotapi": have_module("spotapi"), "spotdl": have_module("spotdl"),
            "ffmpeg": bool(shutil.which("ffmpeg")), "ok": True}


# ───────────────────────────── поиск ─────────────────────────────
def http_json(url, timeout=12):
    req = urllib.request.Request(url, headers={"User-Agent": "Spotiloss/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def item(id_, title, artist, cover, dur, query):
    return {"id": str(id_), "title": title, "artist": artist, "cover": cover or "",
            "duration": int(dur or 0), "query": query}


def s_spotapi(q, limit):
    from spotapi import Song
    data = Song().query_songs(q, limit=limit)
    out = []
    for it in data["data"]["searchV2"]["tracksV2"]["items"]:
        d = it["item"]["data"]
        tid = d.get("id") or d["uri"].split(":")[-1]
        srcs = ((d.get("albumOfTrack") or {}).get("coverArt") or {}).get("sources") or []
        cover = max(srcs, key=lambda s: s.get("width") or 0)["url"] if srcs else ""
        artists = ", ".join(a["profile"]["name"] for a in (d.get("artists") or {}).get("items", []))
        out.append(item(tid, d["name"], artists, cover,
                        ((d.get("duration") or {}).get("totalMilliseconds") or 0) // 1000,
                        "https://open.spotify.com/track/" + tid))
    return out


def s_deezer(q, limit):
    res = http_json("https://api.deezer.com/search?" + urllib.parse.urlencode({"q": q, "limit": limit}))["data"]
    return [item(x["id"], x["title"], x["artist"]["name"], x["album"].get("cover_big"), x.get("duration"),
                 f'{x["artist"]["name"]} - {x["title"]}') for x in res]


def s_itunes(q, limit):
    res = http_json("https://itunes.apple.com/search?" + urllib.parse.urlencode(
        {"term": q, "media": "music", "entity": "song", "limit": limit}))["results"]
    return [item(x["trackId"], x["trackName"], x["artistName"],
                 x.get("artworkUrl100", "").replace("100x100", "600x600"),
                 (x.get("trackTimeMillis") or 0) // 1000, f'{x["artistName"]} - {x["trackName"]}') for x in res]


def search(q, limit=24):
    errors = []
    for name, fn in (("spotapi", s_spotapi), ("deezer", s_deezer), ("itunes", s_itunes)):
        try:
            res = POOL.submit(fn, q, limit).result(timeout=10)  # SpotAPI иногда виснет — ждём максимум 10 с
            if res:
                return res, name
        except Exception as e:
            errors.append(f"{name}: {str(e)[:80]}")
            print("поиск:", errors[-1])
    raise RuntimeError("поиск не сработал (" + "; ".join(errors) + "). Проверь интернет.")


# ──────────────────────────── загрузка ────────────────────────────
def find_audio(folder):
    if folder.is_dir():
        for f in sorted(folder.iterdir()):
            if f.suffix.lower() in AUDIO and f.stat().st_size > 0:
                return f
    return None


def clean_cache():
    try:
        dirs = [d for d in ROOT.iterdir() if d.is_dir()]
        size = lambda d: sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
        total = sum(size(d) for d in dirs)
        for d in sorted(dirs, key=lambda d: d.stat().st_mtime):
            if total <= MAX_MB * 1024 * 1024:
                break
            total -= size(d)
            shutil.rmtree(d, ignore_errors=True)
    except Exception as e:
        print("очистка кэша:", e)


def set_job(jid, **kw):
    with LOCK:
        JOBS[jid].update(kw)
        JOBS[jid]["t"] = time.time()


def run_job(jid, query, quality):
    folder = ROOT / hashlib.md5(f"{query}|{quality}".encode()).hexdigest()[:16]
    try:
        f = find_audio(folder)
        if not f:
            if not have_module("spotdl"):
                raise RuntimeError("spotDL не установлен. Выполни: pip install spotdl")
            if not shutil.which("ffmpeg"):
                raise RuntimeError("Не найден ffmpeg (Termux: pkg install ffmpeg)")
            set_job(jid, status="queued", msg="В очереди…")
            with SLOTS:
                set_job(jid, status="running", msg="Скачиваю…")
                folder.mkdir(parents=True, exist_ok=True)
                cmd = [sys.executable, "-m", "spotdl", "download", query,
                       "--output", str(folder / "{title}.{output-ext}"),
                       "--format", "mp3", "--bitrate", quality]
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace")
                tail, start = [], time.time()
                killer = threading.Timer(600, p.kill)
                killer.start()
                for line in p.stdout:
                    line = line.strip()
                    if line:
                        tail = (tail + [line])[-6:]
                        set_job(jid, msg=line[:120])
                p.wait()
                killer.cancel()
            f = find_audio(folder)
            if not f:
                shutil.rmtree(folder, ignore_errors=True)
                raise RuntimeError(" | ".join(tail)[-300:] or "spotDL ничего не скачал")
            clean_cache()
        set_job(jid, status="done", url="/files/" + folder.name + "/" + urllib.parse.quote(f.name))
    except Exception as e:
        set_job(jid, status="error", error=str(e))


def start_job(query, quality):
    key = hashlib.md5(f"{query}|{quality}".encode()).hexdigest()
    with LOCK:  # тот же трек уже качается — не запускаем второй раз
        for jid, j in JOBS.items():
            if j.get("key") == key and j.get("status") in ("queued", "running"):
                return jid
        now = time.time()
        for jid in [k for k, v in JOBS.items() if now - v.get("t", now) > 3600]:
            del JOBS[jid]
        jid = uuid.uuid4().hex[:10]
        JOBS[jid] = {"status": "queued", "key": key, "t": now, "msg": "В очереди…"}
    threading.Thread(target=run_job, args=(jid, query, quality), daemon=True).start()
    return jid


# ───────────────────────────── HTTP ─────────────────────────────
class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Range")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS")

    def reply(self, obj, code=200):
        b = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_OPTIONS(self):
        self.send_response(204)
        self.cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        self.do_GET(head=True)

    def do_GET(self, head=False):
        u = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(u.query)
        if u.path in ("/", "/index.html", "/spotiloss.html"):
            return self.send_file(HERE / "spotiloss.html", head)
        if u.path == "/api/ping":
            return self.reply({"ok": True})
        if u.path == "/api/health":
            return self.reply(health())
        if u.path == "/api/search":
            q = (qs.get("q") or [""])[0].strip()
            if not q:
                return self.reply({"items": []})
            try:
                limit = max(1, min(int((qs.get("limit") or ["24"])[0]), 50))
                items, src = search(q, limit)
                return self.reply({"items": items, "source": src})
            except Exception as e:
                return self.reply({"error": str(e)}, 502)
        if u.path.startswith("/api/job/"):
            with LOCK:
                j = dict(JOBS.get(u.path.rsplit("/", 1)[1], {"status": "error", "error": "задача не найдена"}))
            j.pop("key", None)
            return self.reply(j)
        if u.path.startswith("/files/"):
            p = (ROOT / urllib.parse.unquote(u.path[7:])).resolve()
            if ROOT.resolve() not in p.parents:
                return self.reply({"error": "not found"}, 404)
            return self.send_file(p, head)
        self.reply({"error": "not found"}, 404)

    def do_POST(self):
        if self.path != "/api/download":
            return self.reply({"error": "not found"}, 404)
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            query = str(body["query"]).strip()[:300]
            quality = str(body.get("quality", "128k"))
            if not query:
                raise ValueError
            if not re.fullmatch(r"\d{2,3}k", quality):
                quality = "128k"
        except Exception:
            return self.reply({"error": "неверный запрос"}, 400)
        self.reply({"job": start_job(query, quality)})

    def send_file(self, p, head=False):
        if not p.is_file():
            return self.reply({"error": "файл не найден"}, 404)
        size = p.stat().st_size
        start, end, code = 0, size - 1, 200
        m = re.match(r"bytes=(\d*)-(\d*)", self.headers.get("Range") or "")
        if m and (m.group(1) or m.group(2)):  # перемотка
            if m.group(1):
                start = int(m.group(1))
                if m.group(2):
                    end = min(int(m.group(2)), size - 1)
            else:
                start = max(0, size - int(m.group(2)))
            if start > end or start >= size:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            code = 206
        self.send_response(code)
        self.cors()
        self.send_header("Content-Type", MIME.get(p.suffix.lower(), "application/octet-stream"))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(end - start + 1))
        if code == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        if head:
            return
        with open(p, "rb") as f:
            f.seek(start)
            left = end - start + 1
            while left > 0:
                chunk = f.read(min(65536, left))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    break
                left -= len(chunk)


def main():
    global ROOT, MAX_MB
    ap = argparse.ArgumentParser(description="Spotiloss сервер")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--dir", default=str(HERE / "downloads"), help="куда складывать треки")
    ap.add_argument("--max-mb", type=int, default=MAX_MB, help="лимит папки с треками, МБ")
    a = ap.parse_args()
    ROOT, MAX_MB = Path(a.dir).resolve(), a.max_mb
    ROOT.mkdir(parents=True, exist_ok=True)
    h = health()
    print("Spotiloss сервер запущен")
    print(f"  Открой:  http://localhost:{a.port}")
    print(f"  Треки:   {ROOT}")
    print("  SpotAPI: " + ("есть" if h["spotapi"] else "нет (будет Deezer/iTunes) — pip install spotapi"))
    print("  spotDL:  " + ("есть" if h["spotdl"] else "НЕТ — pip install spotdl"))
    print("  ffmpeg:  " + ("есть" if h["ffmpeg"] else "НЕТ — без него скачивание не работает"))
    try:
        ThreadingHTTPServer((a.host, a.port), H).serve_forever()
    except KeyboardInterrupt:
        print("\nСервер остановлен")


if __name__ == "__main__":
    main()
