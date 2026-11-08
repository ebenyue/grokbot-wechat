#!/usr/bin/env python3
"""Debug tool: fetch updates once (long-poll ~35s). Saves cursor + the owner's context_token to state.json.
Prints ids and types only. Does not print context tokens, message text, or raw error bodies.
WARNING: do NOT run this while forward.py is running. getupdates is cursor-based; whatever poll.py
consumes is gone for forward.py (the messages never reach the agent).
Usage: python3 poll.py [--loop]"""
import json, sys
import ilink
from ilink import post, load_state, save_state, updates_status, norm_id, clamp_longpoll_ms

def once():
    if ilink.session_paused():
        print("(session paused)"); return []
    st = load_state()
    timeout = max(clamp_longpoll_ms(st.get("longpolling_timeout_ms")) / 1000.0, 1.0)
    try:
        raw = post("ilink/bot/getupdates", {"get_updates_buf": st.get("get_updates_buf", "")}, timeout=timeout)
    except (TimeoutError,):
        print("(timeout, no messages)"); return []
    except ilink.IlinkError as e:
        print("ERROR type=%s status=%s" % (e.kind, e.status)); return []
    except Exception as e:
        print("ERROR type=%s" % type(e).__name__); return []
    try: r = json.loads(raw)
    except ValueError:
        print("ERROR type=non-json"); return []
    status = updates_status(r)
    if status == "stale":
        ilink.pause_session(); print("ERROR stale ret=%s errcode=%s" % (r.get("ret"), r.get("errcode"))); return []
    if status != "ok":
        print("ERROR ret=%s errcode=%s" % (r.get("ret"), r.get("errcode"))); return []
    st = load_state()
    if r.get("get_updates_buf"): st["get_updates_buf"] = r["get_updates_buf"]
    if r.get("longpolling_timeout_ms"):
        st["longpolling_timeout_ms"] = clamp_longpoll_ms(r["longpolling_timeout_ms"])
    msgs = r.get("msgs") or []
    try: owner = ilink.owner_id()
    except ilink.OwnerMissing: owner = None
    kept = []
    for m in msgs:
        mid = norm_id(m.get("message_id"))
        types = [i.get("type") for i in m.get("item_list") or []]
        match = owner is not None and m.get("message_type") == 1 and m.get("from_user_id") == owner and not m.get("group_id")
        print(json.dumps({"message_id": mid, "message_type": m.get("message_type"), "item_types": types,
                          "owner_match": bool(match)}, ensure_ascii=False))
        if not match: continue
        st["last_from_user_id"] = owner
        if m.get("context_token"):
            st.setdefault("context_tokens", {})[owner] = m["context_token"]
        kept.append(m)
    st["raw_log"] = ((st.get("raw_log") or []) + kept)[-20:]
    save_state(st)
    if not msgs: print("(no messages)")
    return msgs

if __name__ == "__main__":
    if "--loop" in sys.argv:
        while True: once()
    else: once()
