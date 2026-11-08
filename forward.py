#!/usr/bin/env python3
"""Forward the scan-bound owner's WeChat ClawBot messages to a Grok Bot webhook routine.
Loops on ilink/bot/getupdates. The cursor is saved only after pending messages and seen ids are durable.
Three threads:
  main     long-poll getupdates -> owner filter -> dedupe -> media -> pending.json
  flusher  POST pending.json as one batch once the user pauses (BATCH_QUIET_S) or after BATCH_CAP_S
  outbox   flush queued definite failures after the owner writes; one keep-alive nudge near the window end
Usage: python3 forward.py [--selftest]"""
import json, os, socket, sys, threading, time, logging, urllib.request, urllib.error
import ilink
import config, media, history, outbox
from config import webhook_url, webhook_key, OWNER_NAME
from ilink import DIR, load_state, save_state, atomic_json, updates_status, norm_id, clamp_longpoll_ms, redact

SEEN = os.path.join(DIR, "forward_seen.json")      # recent message_ids, for dedupe
HEARTBEAT = os.path.join(DIR, "forward.heartbeat") # plain epoch while polling; "paused <until> <written>" during -14
TRANSIENT_CAP = 30                                 # outer webhook retries at 60s, then hold until process restart
logging.basicConfig(filename=os.path.join(DIR, "forward.log"), level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("forward")
try: os.chmod(os.path.join(DIR, "forward.log"), 0o600)
except OSError: pass

def _permanent(res):
    if not res or not isinstance(res, tuple) or not isinstance(res[0], int): return False
    code = res[0]
    if code in (301, 302, 303, 307, 308): return True
    return 400 <= code < 500 and code not in (408, 429)

def post_webhook(payload, tries=5, sleep=time.sleep):
    try:
        url = ilink.https_webhook(webhook_url())
    except Exception:
        log.error("webhook url missing or rejected"); return (400, "")
    key = webhook_key()
    if not key:
        log.error("WECHAT_WEBHOOK_KEY unavailable; cannot forward"); return (400, "")
    data = json.dumps(payload, ensure_ascii=False).encode()
    for i in range(tries):
        try:
            req = urllib.request.Request(url, data=data, method="POST",
                headers={"Content-Type": "application/json", "Authorization": "Bearer " + key})
            with ilink.OPENER.open(req, timeout=20) as f:
                f.read()  # response body is not logged
                log.info("webhook OK status=%s", f.status)
                return (f.status, "")
        except urllib.error.HTTPError as e:
            log.warning("webhook HTTP %s try=%d", e.code, i + 1)
            try: e.read()
            except Exception: pass
            if _permanent((e.code, "")): return (e.code, "")
        except Exception as e:
            log.warning("webhook error try=%d type=%s", i + 1, type(e).__name__)
        sleep(min(2 ** i * 2, 60))
    log.error("webhook stopped after %d tries", tries)
    return None

def load_seen():
    """Ids already accepted. Pending ids are included so a crash after enqueue still dedups.
    The list is not trimmed: a capped list would forget ids the server can replay."""
    ids = []
    try:
        with open(SEEN, encoding="utf-8") as f:
            ids = [norm_id(x) for x in json.load(f) if norm_id(x)]
    except Exception:
        ids = []
    try:
        with open(PENDING, encoding="utf-8") as f:
            pending = json.load(f)
        for m in pending.get("messages") or []:
            mid = norm_id(m.get("message_id"))
            if mid and mid not in ids:
                ids.append(mid)
    except Exception:
        pass
    return ids

def merge_seen(seen, new_ids):
    seen_set = set(seen)
    merged = list(seen)
    for mid in new_ids:
        if mid and mid not in seen_set:
            merged.append(mid)
            seen_set.add(mid)
    return merged

def save_seen(ids):
    atomic_json(SEEN, ids)

def heartbeat_text(now=None):
    """Plain epoch while polling. A paused line is not a healthy heartbeat."""
    now = int(time.time() if now is None else now)
    if ilink.session_paused(now):
        until = int(ilink.load_state().get("session_pause_until") or 0)
        return "paused %d %d" % (until, now)
    return str(now)

def write_heartbeat(now=None):
    line = heartbeat_text(now)
    with open(HEARTBEAT, "w", encoding="utf-8") as f:
        f.write(line + "\n")
    os.chmod(HEARTBEAT, 0o600)
    return line

def heartbeat_state(text=None, now=None, stale_s=600):
    """ok, starting, paused, stale, or unknown. paused is never ok."""
    now = int(time.time() if now is None else now)
    if text is None:
        try:
            with open(HEARTBEAT, encoding="utf-8") as f:
                text = f.read()
        except OSError:
            return "starting"
    parts = (text or "").split()
    if not parts:
        return "starting"
    if parts[0] == "paused":
        return "paused"
    try:
        written = int(parts[0])
    except ValueError:
        return "unknown"
    if now - written > stale_s:
        return "stale"
    return "ok"

def current_timeout_s():
    ms = clamp_longpoll_ms(load_state().get("longpolling_timeout_ms"))
    return max(ms / 1000.0, 1.0)

def fetch(timeout):
    """One long-poll. Returns (status, obj). Does not save the cursor."""
    if ilink.session_paused(): return "paused", None
    st = load_state()
    try:
        raw = ilink.post("ilink/bot/getupdates", {"get_updates_buf": st.get("get_updates_buf", "")}, timeout=timeout)
    except (socket.timeout, TimeoutError):
        return "timeout", None
    except ilink.SessionPaused:
        return "paused", None
    except ilink.SessionStale:
        return "stale", None
    except Exception as e:
        log.warning("poll error type=%s", getattr(e, "kind", type(e).__name__)); return "error", None
    try: obj = json.loads(raw)
    except ValueError:
        log.warning("getupdates non-json"); return "error", None
    status = updates_status(obj)
    if status == "stale":
        ilink.pause_session()
        log.error("getupdates stale ret=%s errcode=%s", obj.get("ret"), obj.get("errcode"))
        return "stale", None
    if status == "error":
        log.warning("getupdates ret=%s errcode=%s errmsg=%s", obj.get("ret"), obj.get("errcode"), redact(obj.get("errmsg")))
        return "error", None
    return "ok", obj

ATTACH_DIR = config.ATTACH_DIR
MEDIA_TYPES = (2, 3, 4, 5)  # image, voice, file, video

def build_payload(m, attach_dir=ATTACH_DIR, fetch=True):
    items = m.get("item_list") or []
    payload = {"source": "wechat-clawbot", "from_user_id": m.get("from_user_id"),
               "texts": [t for t in (media.summarize(i).get("text") for i in items) if t],
               "item_types": [i.get("type") for i in items],
               "items": [media.summarize(i) for i in items],
               "create_time_ms": m.get("create_time_ms"), "message_id": norm_id(m.get("message_id"))}
    media_items = [i for i in items if i.get("type") in MEDIA_TYPES]
    if media_items:
        payload["has_media"] = True
        files, errors, raw = [], [], []
        ts = (m.get("create_time_ms") or 0) / 1000 or None
        for idx, i in enumerate(items):
            if i.get("type") not in MEDIA_TYPES or not fetch: continue
            try:
                path = media.fetch_item(i, attach_dir, ts)
                files.append(path); payload["items"][idx]["file"] = path
            except media.MediaHostError:
                err = "%s: rejected-host" % media.TYPE_NAMES.get(i.get("type"))
                errors.append(err); payload["items"][idx]["media_error"] = err
            except Exception as e:
                err = "%s: %s" % (media.TYPE_NAMES.get(i.get("type")), type(e).__name__)
                errors.append(err); raw.append(i); payload["items"][idx]["media_error"] = err
        payload["files"] = files
        if errors:
            payload["media_error"] = "; ".join(errors)
            if raw: payload["raw_items"] = raw
            log.warning("media error message_id=%s", payload.get("message_id"))
    return payload

PENDING = os.path.join(DIR, "pending.json")
QUIET_S = config.BATCH_QUIET_S   # flush when no new user message for this long
CAP_S = config.BATCH_CAP_S       # ...or when oldest pending message is this old
RETRY_S = 60                     # wait after a transient flush failure before retrying
KEEPALIVE_TOKENS = {"1", "１", "续", "续一下", "+1", "11", "111"}   # bare "renew the window" replies

class Batcher:
    """Thread-safe, disk-persisted pending buffer, flushed by a background thread.
    Permanent webhook statuses are held for this process. A new process may try again.
    Transient failures retry with the existing backoff until transient_cap, then hold."""
    def __init__(self, path=PENDING, quiet=QUIET_S, cap=CAP_S, poster=None, clock=time.time,
                 history_path=None, transient_cap=TRANSIENT_CAP):
        self.path, self.quiet, self.cap, self.clock = path, quiet, cap, clock
        self.history_path = history_path or history.HISTORY
        self.poster = poster or post_webhook
        self.transient_cap = transient_cap
        self.lock = threading.Lock()
        self.next_try = 0
        self.hold = None
        self.outer = 0
        d = {}
        try:
            with open(path, encoding="utf-8") as f: d = json.load(f)
        except Exception: pass
        self.msgs = d.get("messages", [])
        self.oldest = d.get("oldest")
        # on restart, leftover buffer flushes after a fresh quiet window; hold is not restored
        self.last = self.clock() if self.msgs else None
        if self.msgs: self.oldest = self.last
        if self.msgs: log.info("restored %d pending message(s) from disk", len(self.msgs))

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"messages": self.msgs, "oldest": self.oldest, "last": self.last, "hold": self.hold},
                      f, ensure_ascii=False)
            f.flush(); os.fsync(f.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)

    def _add_unlocked(self, payload):
        mid = norm_id(payload.get("message_id"))
        if mid is not None and any(norm_id(m.get("message_id")) == mid for m in self.msgs):
            return
        now = self.clock()
        if not self.msgs: self.oldest = now
        self.msgs.append(payload); self.last = now
        self._save()
        log.info("buffered message_id=%s (pending=%d)", mid, len(self.msgs))

    def add(self, payload):
        with self.lock:
            self._add_unlocked(payload)

    def accept(self, payloads, after_add):
        """Add payloads and run after_add while the flusher cannot run. after_add persists seen ids."""
        with self.lock:
            for payload in payloads:
                self._add_unlocked(payload)
            after_add()

    def due(self):
        now = self.clock()
        return bool(self.msgs) and not self.hold and now >= self.next_try and (
            now - self.last >= self.quiet or now - self.oldest >= self.cap)

    def maybe_flush(self):
        with self.lock:
            if self.hold or not self.due(): return False
            batch = list(self.msgs)
            reason = "quiet" if self.clock() - self.last >= self.quiet else "cap"
        trigger_ms = max([m.get("create_time_ms") or 0 for m in batch] or [0])
        body = {"source": "wechat-clawbot", "batch": True, "messages": batch, "count": len(batch),
                "trigger_ms": trigger_ms,
                "reply_rule": "Pass --reply-to-ms %d to every send.py call. Exit code 4 (SUPERSEDED) means %s "
                              "sent something newer: stop immediately, send nothing, file nothing; the newer run "
                              "handles it. Exit code 3 means a definite WeChat refusal was queued; do not retry it. "
                              "Exit code 5 means the send was ambiguous: do not call send.py again for that attempt. "
                              "Exit code 6 means the send was refused before it started."
                              % (trigger_ms, OWNER_NAME),
                "history": history.tail(40, self.history_path)}
        hist_lines = [l for l in body["history"].splitlines() if l.strip()]
        after_nudge = bool(hist_lines) and outbox.NUDGE_TEXT in hist_lines[-1]
        first = batch[0]
        gap_h = ((first.get("create_time_ms") or 0) - (first.get("prev_inbound_ms") or 0)) / 3.6e6 if first.get("prev_inbound_ms") else 0
        bare_ping = all("".join(p.get("texts") or []).strip() in KEEPALIVE_TOKENS for p in batch)
        if not after_nudge and bare_ping and gap_h >= 12:
            body["context_note"] = ("%s sent a bare '%s' after %.0fh of silence. That's him renewing the ~24h WeChat push "
                                    "window (the keepalive nudge may have been skipped or missed). Reply with a very short "
                                    "natural ack like '续上了', and if any queued pushes went out, say so briefly. Don't ask "
                                    "what it means." % (OWNER_NAME, "".join(first.get("texts") or []).strip(), gap_h))
        if after_nudge:
            body["context_note"] = ("Your last WeChat message to %s was the keepalive nudge (see the end of history), "
                                    "so this batch is his reply to it. A bare '1' or similar just renews the window: reply "
                                    "with a very short natural ack (e.g. '续上了'), don't ask what's up. If he says more, "
                                    "answer that normally with the nudge as context." % OWNER_NAME)
        log.info("flushing %d message(s) (%s) ids=%s", len(batch), reason, [m.get("message_id") for m in batch])
        res = self.poster(body)
        ok = bool(res) and isinstance(res[0], int) and 200 <= res[0] < 300
        with self.lock:
            if ok:
                try: history.append(history.user_line(batch), self.history_path)
                except Exception as e: log.warning("history append failed type=%s", type(e).__name__)
                self.msgs = self.msgs[len(batch):]
                self.oldest = self.clock() if self.msgs else None
                self.next_try = 0; self.outer = 0; self.hold = None
            elif _permanent(res):
                self.hold = "permanent"
                log.error("webhook permanent status=%s; holding %d pending", res[0], len(self.msgs))
            else:
                self.outer += 1
                if self.outer >= self.transient_cap:
                    self.hold = "transient-budget"
                    log.error("webhook transient budget used; holding %d pending", len(self.msgs))
                else:
                    self.next_try = self.clock() + RETRY_S
                    log.error("flush failed; keeping %d pending, retry in %ds", len(self.msgs), RETRY_S)
            self._save()
        return ok

    def run(self, interval=1.0):
        while True:
            try: self.maybe_flush()
            except Exception as e: log.warning("flusher error type=%s", type(e).__name__)
            time.sleep(interval)

def handle_updates(obj, batcher):
    """Filter to the owner, persist pending and seen ids, then save the cursor.
    Returns 'ok', 'error', 'stale', or 'no-owner'. A failure while adding does not advance the cursor."""
    if updates_status(obj) != "ok": return updates_status(obj)
    try:
        owner = ilink.owner_id()
    except ilink.OwnerMissing:
        log.error("owner id missing; not accepting updates")
        return "no-owner"
    msgs = obj.get("msgs") or []
    seen = load_seen(); seen_set = set(seen)
    st = load_state()
    pending_adds, new_ids = [], []
    for m in msgs:
        if not isinstance(m, dict): continue
        mid = norm_id(m.get("message_id"))
        if mid is not None and mid in seen_set: continue
        if m.get("group_id"):
            if mid is not None: new_ids.append(mid)
            log.info("ignored group message_id=%s", mid); continue
        if m.get("message_type") != 1 or m.get("from_user_id") != owner:
            if mid is not None: new_ids.append(mid)
            log.info("ignored message_id=%s owner_match=no", mid); continue
        prev = st.get("last_inbound_ms")
        owned = dict(m); owned["_prev_inbound_ms"] = prev
        payload = build_payload(owned)
        payload["message_id"] = mid
        payload["prev_inbound_ms"] = prev
        pending_adds.append(payload)
        if mid is not None: new_ids.append(mid)
        st["last_from_user_id"] = owner
        if m.get("context_token"):
            st.setdefault("context_tokens", {})[owner] = m["context_token"]
        ct = m.get("create_time_ms") or int(time.time() * 1000)
        st["last_inbound_ms"] = max(st.get("last_inbound_ms") or 0, ct)
        stored = dict(m)
        st["raw_log"] = ((st.get("raw_log") or []) + [stored])[-20:]
        log.info("received message_id=%s types=%s files=%s", mid, payload["item_types"], len(payload.get("files") or []))
    merged = merge_seen(seen, new_ids)
    batcher.accept(pending_adds, lambda: save_seen(merged))
    if obj.get("get_updates_buf"): st["get_updates_buf"] = obj["get_updates_buf"]
    if obj.get("longpolling_timeout_ms"):
        st["longpolling_timeout_ms"] = clamp_longpoll_ms(obj["longpolling_timeout_ms"])
    save_state(st)
    return "ok"

def failure_sleep(fails):
    return 30 if fails >= 3 else 2

def outbox_loop():
    """Flush queued sends right after the owner writes; nudge near window end."""
    seen_last = None
    while True:
        try:
            last = outbox.last_inbound_s()
            if last and last != seen_last:
                seen_last = last
                time.sleep(3)           # let the owner's message register first
                outbox.flush()
            outbox.maybe_nudge()
        except Exception as e:
            log.warning("outbox loop error type=%s", type(e).__name__)
        time.sleep(15)

def main():
    log.info("forwarder start pid=%d url_set=%s key_present=%s quiet=%ds cap=%ds", os.getpid(),
             bool(webhook_url()), bool(webhook_key()), QUIET_S, CAP_S)
    outbox.recover_inflight()
    batcher = Batcher()
    threading.Thread(target=batcher.run, daemon=True, name="flusher").start()
    threading.Thread(target=outbox_loop, daemon=True, name="outbox").start()
    fails = 0
    while True:
        try:
            write_heartbeat()
        except OSError: pass
        if ilink.session_paused():
            time.sleep(15); continue
        status, obj = fetch(current_timeout_s())
        if status in ("timeout", "paused", "stale"):
            fails = 0; continue
        if status != "ok":
            fails += 1; time.sleep(failure_sleep(fails)); continue
        fails = 0
        try:
            result = handle_updates(obj, batcher)
            if result == "no-owner":
                fails += 1; time.sleep(failure_sleep(fails))
        except Exception as e:
            log.warning("handle error type=%s", type(e).__name__)
            time.sleep(2)

if __name__ == "__main__":
    if "--selftest" in sys.argv:
        res = post_webhook({"source": "wechat-clawbot", "test": True,
                            "texts": ["(forwarder self-test, ignore)"]}, tries=1)
        print("selftest status=%s" % (res[0] if res else "failed"))
    else:
        main()
