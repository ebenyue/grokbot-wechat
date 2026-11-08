"""Outbox for app-initiated WeChat sends + keep-alive nudge.
WeChat iLink only lets the bot message the user for a while (~24h, observed) after the user's
last message. A definite refusal is queued in outbox.json and flushed when the owner writes again.
The queue stores the owner id and an explicit empty retry context, so a later sender cannot retarget it.
A timeout, empty body, or non-JSON result is stored as ambiguous and is not flushed.
Near the end of the window, one short nudge asks the owner to reply so the window stays open.
No nudge during quiet hours (default 00:00-07:59 Beijing); if the normal 18-23h nudge slot would
fall inside quiet hours, the nudge is sent early (as early as 12h) just before quiet hours begin."""
import json, os, time, logging, secrets, threading, fcntl
from datetime import datetime
from zoneinfo import ZoneInfo
from config import QUIET_TZ, NUDGE_TEXT, quiet_hours
from ilink import DIR, load_state, save_state, dispatch_text, owner_id, OwnerMismatch, SessionPaused
BJ = ZoneInfo(QUIET_TZ)

OUTBOX = os.path.join(DIR, "outbox.json")   # queued texts; may contain private content, NEVER commit
NUDGE_AFTER_S = 18 * 3600   # observed: 11h ok, 35h failed; stay well inside
NUDGE_BEFORE_S = 23 * 3600  # past this the nudge itself would likely fail
NUDGE_EARLIEST_S = 12 * 3600  # if the 18-23h window hits quiet hours, nudge early (11h observed ok)
NUDGE_MARGIN_S = 30 * 60      # send this long before quiet hours begin
QUIET_HOURS = quiet_hours()   # fixed zone (QUIET_TZ), independent of the machine TZ
log = logging.getLogger("forward")
_R = threading.RLock()
_depth = 0
_lockf = None

def _pause(seconds):
    time.sleep(seconds)

def _load():
    try:
        with open(OUTBOX, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save(q):
    tmp = OUTBOX + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(q, f, ensure_ascii=False, indent=1)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, 0o600)
    os.replace(tmp, OUTBOX)

class _Guard:
    def __enter__(self):
        global _depth, _lockf
        _R.acquire()
        if _depth == 0:
            _lockf = open(OUTBOX + ".lock", "a+")
            fcntl.flock(_lockf, fcntl.LOCK_EX)
            try: os.chmod(OUTBOX + ".lock", 0o600)
            except OSError: pass
        _depth += 1
        return self
    def __exit__(self, *a):
        global _depth, _lockf
        _depth -= 1
        if _depth == 0 and _lockf is not None:
            fcntl.flock(_lockf, fcntl.LOCK_UN)
            _lockf.close()
            _lockf = None
        _R.release()

def guard():
    return _Guard()

def new_client_id():
    return "openclaw-weixin-" + secrets.token_hex(8)

def recover_inflight():
    """A crash after a request was written but before its result was stored must not be sent again."""
    with guard():
        q = _load(); changed = False
        for item in q:
            if item.get("state") == "inflight":
                item["state"] = "ambiguous"; changed = True
        if changed:
            _save(q)
            log.warning("outbox marked %d in-flight attempt(s) ambiguous", sum(1 for i in q if i.get("state") == "ambiguous"))

def _require_owner(to):
    oid = owner_id()
    if to not in (None, "", oid): raise OwnerMismatch()
    return oid

def enqueue(text, to, media=None, prepared=None, context_token=""):
    with guard():
        q = _load()
        item = {"state": "queued", "text": text or "", "to": to, "context_token": context_token or "",
                "queued_at": time.time(), "client_id": new_client_id()}
        if media: item["media"] = media
        if prepared: item["prepared"] = prepared
        q.append(item); _save(q)
        return item

def _set_state(client_id, state, prepared=None):
    q = _load()
    for item in q:
        if item.get("client_id") == client_id:
            if state is None:
                q.remove(item)
            else:
                item["state"] = state
                if prepared is not None: item["prepared"] = prepared
                if state == "queued": item["context_token"] = ""
            _save(q); return item
    return None

def send_or_queue(text, to=None, context_token=None, queue_rejection=True):
    """Send text to the owner. Definite refusal is queued when queue_rejection is set.
    Ambiguous results are recorded and are not retried. The nudge passes queue_rejection=False
    so a refused reminder is not sent later on its own."""
    recover_inflight()
    try:
        oid = _require_owner(to)
    except OwnerMismatch:
        raise
    from ilink import session_paused
    if session_paused():
        if queue_rejection:
            enqueue(text, oid, context_token="")
            return "queued", None
        return "rejected", None
    cid = new_client_id()
    with guard():
        q = _load()
        q.append({"state": "inflight", "text": text, "to": oid,
                  "context_token": "" if context_token == "" else (context_token or ""),
                  "queued_at": time.time(), "client_id": cid})
        _save(q)
    try:
        outcome = dispatch_text(text, to=oid, context_token=context_token, client_id=cid)
    except SessionPaused:
        _set_state(cid, "queued")
        return "queued", None
    except Exception:
        _set_state(cid, "ambiguous")
        from ilink import Outcome
        return "ambiguous", Outcome("ambiguous", client_id=cid)
    if outcome.kind == "ok":
        _set_state(cid, None)
        return "sent", outcome
    if outcome.kind == "ambiguous":
        _set_state(cid, "ambiguous")
        return "ambiguous", outcome
    if queue_rejection:
        _set_state(cid, "queued")
        return "queued", outcome
    _set_state(cid, None)
    return "rejected", outcome

def _dispatch_item(item):
    """Send one queued or in-flight item. Item state is updated. Returns an Outcome."""
    from ilink import Outcome
    if item.get("prepared"):
        import media
        return media.send_prepared(item["prepared"], caption=item.get("text") or "",
                                   context_token=item.get("context_token") or "", client_id=item.get("client_id"))
    if item.get("media"):
        import media
        kind = item["media"].get("kind")
        path = item["media"].get("path")
        status, prepared = media.upload_plaintext(path, kind, item.get("to"))
        if status == "ok":
            item["prepared"] = prepared
            _set_state(item["client_id"], "inflight", prepared=prepared)
            return media.send_prepared(prepared, caption=item.get("text") or "",
                                       context_token=item.get("context_token") or "", client_id=item.get("client_id"))
        if status in ("ambiguous", "hostile"):
            return Outcome("ambiguous", client_id=item.get("client_id"))
        return Outcome("rejected", client_id=item.get("client_id"))
    return dispatch_text(item.get("text") or "", to=item.get("to"),
                         context_token=item.get("context_token") or "", client_id=item.get("client_id"))

def flush():
    """Send queued messages in order; stop at the first definite failure or ambiguous attempt."""
    from ilink import session_paused
    recover_inflight()
    with guard():
        q = _load(); sent = 0; i = 0
        while i < len(q):
            item = q[i]
            if item.get("state") != "queued":
                i += 1; continue
            if session_paused():
                break
            item["state"] = "inflight"
            if not item.get("client_id"): item["client_id"] = new_client_id()
            _save(q)
            try:
                outcome = _dispatch_item(item)
            except SessionPaused:
                item["state"] = "queued"; item["context_token"] = ""; _save(q); break
            except Exception:
                item["state"] = "ambiguous"; _save(q); break
            if outcome.kind == "ok":
                q.pop(i); sent += 1; _save(q); _pause(1.5); continue
            if outcome.kind == "ambiguous":
                item["state"] = "ambiguous"; _save(q); break
            item["state"] = "queued"; item["context_token"] = ""
            if getattr(outcome, "client_id", None): item["client_id"] = outcome.client_id
            _save(q); break
        if sent: log.info("outbox flushed %d message(s), %d left", sent, len(q))
        return sent

def send_media(path, kind, caption="", context_token=None):
    """Upload and send image, video, or file to the owner. Ambiguous CDN or send is not repeated."""
    from ilink import session_paused, Outcome
    import media
    if kind not in media.UPLOAD_KIND: raise ValueError("unsupported media")
    oid = _require_owner(None)
    media_ref = {"kind": kind, "path": path, "file_name": os.path.basename(path)}
    if session_paused():
        enqueue(caption, oid, media=media_ref, context_token="")
        return "queued", Outcome("queued")
    cid = new_client_id()
    with guard():
        q = _load()
        item = {"state": "inflight", "text": caption or "", "to": oid, "context_token": context_token or "",
                "queued_at": time.time(), "client_id": cid, "media": media_ref}
        q.append(item); _save(q)
    try:
        status, prepared = media.upload_plaintext(path, kind, oid)
    except SessionPaused:
        _set_state(cid, "queued")
        return "queued", Outcome("queued", client_id=cid)
    except Exception:
        _set_state(cid, "ambiguous")
        return "ambiguous", Outcome("ambiguous", client_id=cid)
    if status in ("ambiguous", "hostile"):
        _set_state(cid, "ambiguous")
        return "ambiguous", Outcome("ambiguous", client_id=cid)
    if status != "ok":
        _set_state(cid, "queued")
        return "queued", Outcome("queued", client_id=cid)
    _set_state(cid, "inflight", prepared=prepared)
    try:
        outcome = media.send_prepared(prepared, caption=caption, context_token=context_token, client_id=cid)
    except SessionPaused:
        _set_state(cid, "queued", prepared=prepared)
        return "queued", Outcome("queued", client_id=cid)
    except Exception:
        _set_state(cid, "ambiguous", prepared=prepared)
        return "ambiguous", Outcome("ambiguous", client_id=cid)
    if outcome.kind == "ok":
        _set_state(cid, None)
        return "sent", outcome
    if outcome.kind == "ambiguous":
        _set_state(cid, "ambiguous", prepared=prepared)
        return "ambiguous", outcome
    _set_state(cid, "queued", prepared=prepared)
    return "queued", outcome

def last_inbound_s(st=None):
    st = st or load_state()
    if st.get("last_inbound_ms"): return st["last_inbound_ms"] / 1000
    ts = [m.get("create_time_ms") for m in st.get("raw_log", []) if m.get("message_type") == 1 and m.get("create_time_ms")]
    return max(ts) / 1000 if ts else None

def latest_ok_s(last):
    """Latest non-quiet moment before the window closes (last + NUDGE_BEFORE_S)."""
    t = last + NUDGE_BEFORE_S
    for _ in range(48 * 4):
        if datetime.fromtimestamp(t, BJ).hour not in QUIET_HOURS: return t
        t -= 15 * 60
    return t

def maybe_nudge(now=None):
    now = now or time.time()
    st = load_state(); last = last_inbound_s(st)
    if not last: return False
    age = now - last
    if st.get("nudged_for") == last: return False
    if not (NUDGE_EARLIEST_S <= age < NUDGE_BEFORE_S): return False
    if datetime.fromtimestamp(now, BJ).hour in QUIET_HOURS: return False
    if age < NUDGE_AFTER_S and now < latest_ok_s(last) - NUDGE_MARGIN_S: return False
    try:
        status, outcome = send_or_queue(NUDGE_TEXT, context_token="", queue_rejection=False)
    except Exception as e:
        status, outcome = "ambiguous", None
        log.warning("keepalive nudge error type=%s", type(e).__name__)
    st = load_state(); st["nudged_for"] = last; save_state(st)  # one attempt per window, including ambiguous
    ok = status == "sent"
    log.info("keepalive nudge sent=%s age=%.1fh", ok, age / 3600)
    return ok
