"""Minimal WeChat ClawBot iLink client.
Protocol notes: https://chihao.li/blog/en/wechat-clawbot-ilink-protocol and the
Tencent openclaw-weixin client (headers / base_info / send and upload fields).
Sends go only to credentials.json ilink_user_id. HTTP redirects are not followed."""
import base64, json, os, random, re, secrets, socket, time, urllib.error, urllib.request
from urllib.parse import urlsplit
from config import DATA_DIR

DIR = DATA_DIR
CRED = os.path.join(DIR, "credentials.json")   # written by bind.py; NEVER commit
STATE = os.path.join(DIR, "state.json")        # cursor + context tokens + recent raw msgs; NEVER commit
BASE_INFO = {"channel_version": "2.4.9", "bot_agent": "OpenClaw"}
API_SUFFIX = "weixin.qq.com"
CDN_SUFFIX = "cdn.weixin.qq.com"
STALE_CODE = -14
SESSION_PAUSE_S = 3600
DEFAULT_LONGPOLL_MS = 35000
MAX_LONGPOLL_MS = 90000

class IlinkError(Exception):
    def __init__(self, kind, status=None):
        self.kind = kind
        self.status = status
        super().__init__(kind)

class OwnerMissing(IlinkError):
    def __init__(self, kind="owner"):
        super().__init__(kind)

class OwnerMismatch(IlinkError):
    def __init__(self):
        super().__init__("owner-mismatch")

class SessionPaused(IlinkError):
    def __init__(self):
        super().__init__("paused")

class SessionStale(IlinkError):
    def __init__(self):
        super().__init__("stale", STALE_CODE)

class HostRejected(IlinkError):
    def __init__(self, kind="host"):
        super().__init__(kind)

class Outcome:
    def __init__(self, kind, ret=None, errcode=None, errmsg="", message_id=None, client_id=None, stale=False):
        self.kind = kind          # ok, queued, ambiguous, rejected
        self.ret = ret
        self.errcode = errcode
        self.errmsg = errmsg
        self.message_id = message_id
        self.client_id = client_id
        self.stale = stale

    def line(self):
        parts = []
        if self.message_id: parts.append("message_id=%s" % self.message_id)
        if self.client_id: parts.append("client_id=%s" % self.client_id)
        if self.ret is not None or self.errcode is not None or self.errmsg:
            parts.append("ret=%s errcode=%s errmsg=%s" % (self.ret, self.errcode, self.errmsg or ""))
        return " ".join(parts)

_URL_RE = re.compile(r"https?://\S+", re.I)
_BEARER_RE = re.compile(r"Bearer\s+\S+", re.I)

def redact(text, limit=160):
    s = _URL_RE.sub("[url]", str(text or ""))
    s = _BEARER_RE.sub("Bearer [redacted]", s)
    return s[:limit]

def _labels_ok(host):
    if not host or len(host) > 253: return False
    labels = host.split(".")
    for lab in labels:
        if not lab or len(lab) > 63: return False
        if not re.fullmatch(r"[A-Za-z0-9-]+", lab): return False
        if lab.startswith("-") or lab.endswith("-"): return False
    return True

def _host_matches(host, suffix):
    host = host.lower().rstrip(".")
    suffix = suffix.lower().rstrip(".")
    return host == suffix or host.endswith("." + suffix)

def https_api_base(base):
    p = urlsplit((base or "").strip())
    if p.scheme != "https" or p.username or p.password or p.port not in (None, 443):
        raise HostRejected("api")
    if p.query or p.fragment or p.path not in ("", "/"):
        raise HostRejected("api")
    host = (p.hostname or "")
    if not _labels_ok(host) or not _host_matches(host, API_SUFFIX):
        raise HostRejected("api")
    return (base or "").strip().rstrip("/")

def https_cdn(url):
    p = urlsplit((url or "").strip())
    if p.scheme != "https" or p.username or p.password or p.port not in (None, 443):
        raise HostRejected("cdn")
    host = p.hostname or ""
    if not _labels_ok(host) or not _host_matches(host, CDN_SUFFIX):
        raise HostRejected("cdn")
    return (url or "").strip()

def https_webhook(url):
    p = urlsplit((url or "").strip())
    if p.scheme != "https" or not p.hostname or p.username or p.password or p.port not in (None, 443):
        raise HostRejected("webhook")
    if not _labels_ok(p.hostname):
        raise HostRejected("webhook")
    return (url or "").strip()

def clamp_longpoll_ms(ms, default=DEFAULT_LONGPOLL_MS, cap=MAX_LONGPOLL_MS):
    try: n = int(ms)
    except (TypeError, ValueError): return default
    if n <= 0: return default
    return min(n, cap)

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirect refused", headers, fp)

OPENER = urllib.request.build_opener(_NoRedirect)

def atomic_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)

def creds():
    try:
        with open(CRED, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        raise OwnerMissing("credentials") from None

def owner_id(c=None):
    c = c or creds()
    oid = str((c or {}).get("ilink_user_id") or "").strip()
    if not oid: raise OwnerMissing("ilink_user_id")
    return oid

def load_state():
    if not os.path.exists(STATE): return {}
    try:
        with open(STATE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}

def save_state(s):
    atomic_json(STATE, s)

def cred_mark():
    try:
        st = os.stat(CRED)
        return "%d:%d" % (getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)), st.st_size)
    except OSError:
        return ""

def _clear_pause():
    st = load_state()
    if "session_pause_until" in st or "session_pause_mark" in st:
        st.pop("session_pause_until", None)
        st.pop("session_pause_mark", None)
        save_state(st)

def session_paused(now=None):
    now = time.time() if now is None else now
    st = load_state()
    until = st.get("session_pause_until") or 0
    mark = st.get("session_pause_mark") or ""
    current = cred_mark()
    if until and mark and current and current != mark:
        _clear_pause(); return False
    if until and now >= until:
        _clear_pause(); return False
    return bool(until and now < until)

def pause_session(seconds=SESSION_PAUSE_S, now=None):
    now = time.time() if now is None else now
    st = load_state()
    st["session_pause_until"] = now + seconds
    st["session_pause_mark"] = cred_mark()
    save_state(st)

def classify_response(raw):
    """Classify an iLink JSON body. Empty, non-JSON, and non-object bodies are ambiguous.
    A JSON object with no non-zero ret/errcode is success, including {}. Non-zero ret or errcode is rejected.
    errcode or ret -14 is the documented stale-session code."""
    if raw is None: return Outcome("ambiguous")
    if isinstance(raw, bytes): raw = raw.decode("utf-8", "replace")
    text = str(raw)
    if not text.strip(): return Outcome("ambiguous")
    try: obj = json.loads(text)
    except ValueError: return Outcome("ambiguous")
    if not isinstance(obj, dict): return Outcome("ambiguous")
    ret, errcode = obj.get("ret"), obj.get("errcode")
    mid = obj.get("message_id")
    stale = ret == STALE_CODE or errcode == STALE_CODE
    bad = (ret not in (None, 0)) or (errcode not in (None, 0))
    return Outcome("rejected" if bad else "ok", ret=ret, errcode=errcode,
                   errmsg=redact(obj.get("errmsg") or ""),
                   message_id=None if mid is None else str(mid), stale=stale)

def updates_status(obj):
    if not isinstance(obj, dict): return "error"
    if obj.get("ret") == STALE_CODE or obj.get("errcode") == STALE_CODE: return "stale"
    if (obj.get("ret") not in (None, 0)) or (obj.get("errcode") not in (None, 0)): return "error"
    return "ok"

def norm_id(mid):
    if mid is None or mid == "": return None
    return str(mid)

def post(endpoint, body, timeout=15):
    if session_paused(): raise SessionPaused()
    c = creds()
    token = str(c.get("bot_token") or "").strip()
    if not token: raise OwnerMissing("bot_token")
    base = https_api_base(c.get("baseurl") or "https://ilinkai.weixin.qq.com")
    url = base + "/" + endpoint.lstrip("/")
    hdrs = {"Content-Type": "application/json", "AuthorizationType": "ilink_bot_token",
            "X-WECHAT-UIN": base64.b64encode(str(random.getrandbits(32)).encode()).decode(),
            "iLink-App-Id": "bot", "iLink-App-ClientVersion": str((2 << 16) | (4 << 8) | 9),
            "Authorization": "Bearer " + token}
    data = json.dumps(dict(body, base_info=BASE_INFO), ensure_ascii=False).encode()
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    try:
        with OPENER.open(req, timeout=timeout) as f:
            return f.read().decode()
    except urllib.error.HTTPError as e:
        raw = b""
        try: raw = e.read()
        except Exception: pass
        if e.code in (301, 302, 303, 307, 308):
            raise IlinkError("redirect", e.code) from None
        try: obj = json.loads(raw.decode("utf-8", "replace") or "null")
        except Exception: obj = None
        if isinstance(obj, dict) and (obj.get("ret") == STALE_CODE or obj.get("errcode") == STALE_CODE):
            pause_session(); raise SessionStale() from None
        if isinstance(obj, dict):
            return raw.decode("utf-8", "replace")
        raise IlinkError("http", e.code) from None
    except (TimeoutError, socket.timeout):
        raise
    except IlinkError:
        raise
    except Exception as e:
        raise IlinkError(type(e).__name__) from None

def _history_line(text):
    try:
        import history
        history.append(history.grok_line(text))
    except Exception:
        pass

def _context_for(oid, context_token):
    if context_token is None:
        return (load_state().get("context_tokens") or {}).get(oid) or ""
    return context_token or ""

def dispatch_message(item_list, history_text, to=None, context_token=None, client_id=None):
    """Send one finished bot message to the owner. Does not queue. Network uncertainty is ambiguous."""
    oid = owner_id()
    if to not in (None, "", oid): raise OwnerMismatch()
    if session_paused(): raise SessionPaused()
    context_token = _context_for(oid, context_token)
    cid = client_id or ("openclaw-weixin-" + secrets.token_hex(8))
    msg = {"from_user_id": "", "to_user_id": oid, "client_id": cid, "message_type": 2,
           "message_state": 2, "item_list": item_list}
    if context_token: msg["context_token"] = context_token
    try:
        raw = post("ilink/bot/sendmessage", {"msg": msg})
    except (SessionPaused, SessionStale):
        raise
    except Exception:
        return Outcome("ambiguous", client_id=cid)
    outcome = classify_response(raw)
    outcome.client_id = cid
    if outcome.stale: pause_session()
    if outcome.kind == "ok" and history_text: _history_line(history_text)
    return outcome

def dispatch_text(text, to=None, context_token=None, client_id=None):
    return dispatch_message([{"type": 1, "text_item": {"text": text}}], text,
                            to=to, context_token=context_token, client_id=client_id)

def send_text(text, to=None, context_token=None, client_id=None):
    """Compatibility wrapper used by the outbox. Queuing lives in outbox.py."""
    return dispatch_text(text, to=to, context_token=context_token, client_id=client_id)
