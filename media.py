"""WeChat iLink media: summarise, CDN download, AES-128-ECB, and owner-only image/file/video upload.
Upload follows the Tencent openclaw-weixin pipeline: getUploadUrl, encrypt, CDN POST, sendMessage.
Voice upload and thumbnails are not implemented. CDN hosts must be https *.cdn.weixin.qq.com."""
import base64, binascii, hashlib, os, time, urllib.parse, urllib.request
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import ilink

CDN_BASE = "https://novac2c.cdn.weixin.qq.com/c2c"
TYPE_NAMES = {1: "text", 2: "image", 3: "voice", 4: "file", 5: "video"}
ITEM_KEYS = {1: "text_item", 2: "image_item", 3: "voice_item", 4: "file_item", 5: "video_item"}
UPLOAD_KIND = {"image": 1, "video": 2, "file": 3}   # proto UploadMediaType; voice (4) is not sent

class MediaHostError(ValueError):
    pass

def _strip_long(d, maxlen=120):
    if isinstance(d, dict): return {k: _strip_long(v, maxlen) for k, v in d.items()}
    if isinstance(d, list): return [_strip_long(v, maxlen) for v in d]
    if isinstance(d, str) and len(d) > maxlen: return d[:maxlen] + "...(%d chars)" % len(d)
    return d

def summarize(item):
    t = item.get("type"); name = TYPE_NAMES.get(t, "unknown")
    s = {"type": t, "type_name": name}
    if t == 1:
        s["text"] = (item.get("text_item") or {}).get("text")
    else:
        sub = item.get(ITEM_KEYS.get(t, ""), {}) or {}
        meta = {k: v for k, v in sub.items() if k not in ("media", "thumb_media", "aeskey")}
        if t == 3 and sub.get("text"): s["text"] = sub["text"]  # voice transcript
        s["meta"] = _strip_long(meta)
    return s

def parse_key(item_sub):
    """Return 16-byte AES key. aeskey is hex; media.aes_key is base64 of raw 16 bytes or of the hex string."""
    hx = item_sub.get("aeskey")
    if hx:
        return binascii.unhexlify(hx)
    b64 = (item_sub.get("media") or {}).get("aes_key")
    if not b64: raise ValueError("no aes key in item")
    raw = base64.b64decode(b64)
    if len(raw) == 16: return raw
    if len(raw) == 32: return binascii.unhexlify(raw.decode())
    raise ValueError("unexpected aes_key length %d" % len(raw))

def media_url(media):
    if media.get("full_url"):
        try: return ilink.https_cdn(media["full_url"])
        except ilink.HostRejected as e: raise MediaHostError("cdn") from None
    q = media.get("encrypt_query_param")
    if not q: raise ValueError("no media url / encrypt_query_param")
    return CDN_BASE + "/download?encrypted_query_param=" + urllib.parse.quote(q, safe="")

def download(url, timeout=60):
    try: ilink.https_cdn(url)
    except ilink.HostRejected: raise MediaHostError("cdn") from None
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with ilink.OPENER.open(req, timeout=timeout) as f:
        return f.read()

def pkcs7_pad(data):
    pad = 16 - (len(data) % 16)
    return data + bytes([pad]) * pad

def encrypt(data, key):
    e = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
    blob = pkcs7_pad(data)
    return e.update(blob) + e.finalize()

def decrypt(data, key):
    d = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
    out = d.update(data) + d.finalize()
    pad = out[-1] if out else 0
    if 1 <= pad <= 16 and out.endswith(bytes([pad]) * pad): out = out[:-pad]
    return out

MAGIC = [(b"\xff\xd8\xff", "jpg"), (b"\x89PNG", "png"), (b"GIF8", "gif"), (b"RIFF", "webp"), (b"%PDF", "pdf"),
         (b"PK\x03\x04", "zip"), (b"#!AMR", "amr"), (b"\x02#!SILK", "silk"), (b"#!SILK", "silk")]
def sniff_ext(data, default="bin"):
    for m, e in MAGIC:
        if data.startswith(m): return e
    if len(data) >= 12 and data[4:8] == b"ftyp": return "mp4"
    return default

def _image_bytes(data):
    if data.startswith(b"\xff\xd8\xff") or data.startswith(b"\x89PNG") or data.startswith(b"GIF8"):
        return True
    return len(data) >= 12 and data.startswith(b"RIFF") and data[8:12] == b"WEBP"

def _write_private(path, blob):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try: os.chmod(os.path.dirname(path), 0o700)
    except OSError: pass
    with open(path, "wb") as f: f.write(blob)
    os.chmod(path, 0o600)
    return os.path.abspath(path)

def fetch_item(item, outdir, ts=None):
    """Download+decrypt an image/file/voice/video item. Returns absolute saved path.
    An image with no AES key is kept when the bytes are an image, matching the Tencent downloader.
    File, voice, and video without a key are not saved."""
    t = item.get("type"); name = TYPE_NAMES.get(t, "media")
    sub = item.get(ITEM_KEYS.get(t, ""), {}) or {}
    media = sub.get("media") or {}
    blob = download(media_url(media))
    try:
        plain = decrypt(blob, parse_key(sub))
    except ValueError:
        if t == 2 and _image_bytes(blob): plain = blob
        else: raise
    ext = None
    if t == 4 and sub.get("file_name") and "." in sub["file_name"]:
        ext = sub["file_name"].rsplit(".", 1)[1][:8]
    ext = ext or sniff_ext(plain)
    os.makedirs(outdir, exist_ok=True)
    try: os.chmod(outdir, 0o700)
    except OSError: pass
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(ts or time.time()))
    path = os.path.join(outdir, "%s-%s.%s" % (stamp, name, ext)); n = 1
    while os.path.exists(path):
        path = os.path.join(outdir, "%s-%s-%d.%s" % (stamp, name, n, ext)); n += 1
    return _write_private(path, plain)

def _cdn_exchange(url, ciphertext, timeout=60):
    """POST ciphertext. Returns (status, headers). Raises on transport failure."""
    ilink.https_cdn(url)
    req = urllib.request.Request(url, data=ciphertext, method="POST",
                                 headers={"Content-Type": "application/octet-stream"})
    try:
        with ilink.OPENER.open(req, timeout=timeout) as f:
            f.read()
            return f.status, {k.lower(): v for k, v in f.headers.items()}
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in (e.headers.items() if e.headers else [])}

def cdn_upload(url, ciphertext, exchange=None, attempts=3):
    """Upload encrypted bytes. 4xx stops. Timeout does not retry. 5xx and a missing header retry up to 3."""
    exchange = exchange or _cdn_exchange
    try: ilink.https_cdn(url)
    except ilink.HostRejected: return "hostile", None
    last = None
    for _ in range(attempts):
        try:
            status, headers = exchange(url, ciphertext)
        except Exception:
            return "ambiguous", None
        if 400 <= status < 500:
            return "rejected", status
        param = (headers or {}).get("x-encrypted-param")
        if status == 200 and param:
            return "ok", param
        last = status
    return "rejected", last

def _aes_b64(raw_key):
    """Tencent sender stores base64 of the hex key text, which parse_key already accepts."""
    return base64.b64encode(raw_key.hex().encode()).decode()

def _message_item(kind, prepared):
    media = {"encrypt_query_param": prepared["encrypt_query_param"], "aes_key": prepared["aes_key_b64"],
             "encrypt_type": 1}
    if kind == "image":
        return {"type": 2, "image_item": {"media": media, "mid_size": prepared["cipher_size"]}}
    if kind == "video":
        return {"type": 5, "video_item": {"media": media, "video_size": prepared["cipher_size"]}}
    return {"type": 4, "file_item": {"media": media, "file_name": prepared.get("file_name") or "file",
                                    "len": str(prepared["plain_size"])}}

def _history_label(kind, prepared, caption):
    if caption: return caption
    if kind == "image": return "[图片]"
    if kind == "video": return "[视频]"
    return "[文件 %s]" % (prepared.get("file_name") or "")

def upload_plaintext(path, kind, to_user, exchange=None):
    """getUploadUrl + CDN POST. Returns (status, prepared_or_none). status is ok, rejected, ambiguous, hostile."""
    if kind not in UPLOAD_KIND: raise ValueError("unsupported media")
    with open(path, "rb") as f:
        plain = f.read()
    key = os.urandom(16)
    filekey = os.urandom(16).hex()
    cipher = encrypt(plain, key)
    body = {"filekey": filekey, "media_type": UPLOAD_KIND[kind], "to_user_id": to_user,
            "rawsize": len(plain), "rawfilemd5": hashlib.md5(plain).hexdigest(), "filesize": len(cipher),
            "no_need_thumb": True, "aeskey": key.hex()}
    try:
        raw = ilink.post("ilink/bot/getuploadurl", body)
    except (ilink.SessionPaused, ilink.SessionStale):
        raise
    except Exception:
        return "ambiguous", None
    parsed = ilink.classify_response(raw)
    if parsed.stale:
        ilink.pause_session(); return "ambiguous", None
    if parsed.kind != "ok":
        return "rejected", None
    try: info = __import__("json").loads(raw)
    except ValueError:
        return "ambiguous", None
    full = (info.get("upload_full_url") or "").strip()
    param = info.get("upload_param") or ""
    if full:
        url = full
    elif param:
        url = CDN_BASE + "/upload?encrypted_query_param=" + urllib.parse.quote(param, safe="") + "&filekey=" + urllib.parse.quote(filekey, safe="")
    else:
        return "rejected", None
    try: ilink.https_cdn(url)
    except ilink.HostRejected:
        return "hostile", None
    status, download_param = cdn_upload(url, cipher, exchange=exchange)
    if status != "ok":
        return status, None
    name = os.path.basename(path)
    prepared = {"kind": kind, "encrypt_query_param": download_param, "aes_key_b64": _aes_b64(key),
                "cipher_size": len(cipher), "plain_size": len(plain), "file_name": name}
    return "ok", prepared

def send_prepared(prepared, caption="", context_token=None, client_id=None):
    item = _message_item(prepared["kind"], prepared)
    return ilink.dispatch_message([item], _history_label(prepared["kind"], prepared, caption),
                                  context_token=context_token, client_id=client_id)
