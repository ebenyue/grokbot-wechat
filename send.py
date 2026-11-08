#!/usr/bin/env python3
"""Send a text or an image, video, or file to the scan-bound WeChat owner.
Usage: python3 send.py "text" [--to OWNER_ID] [--no-context] [--app] [--reply-to-ms MS]
       python3 send.py --image PATH [--caption TEXT] [--app] [--no-context] [--reply-to-ms MS]
       python3 send.py --video PATH ...
       python3 send.py --file PATH ...
--reply-to-ms: the trigger_ms of the WeChat batch this reply answers. If the owner has sent a newer
  message since, nothing is sent and the exit code is 4 (SUPERSEDED).
--app: prefix APP_PREFIX for pushes not triggered by the WeChat webhook.
--to must be the scan-bound ilink_user_id when it is set.
A definite WeChat refusal is queued (exit 3) and flushed after the owner next writes.
An ambiguous result (exit 5) is recorded and is not sent again automatically.
Exit codes: 0 sent, 3 queued, 4 superseded, 5 ambiguous, 6 refused before send."""
import argparse, os, sys
import outbox
from config import APP_PREFIX, OWNER_NAME
from ilink import OwnerMismatch, OwnerMissing, owner_id

def outgoing_text(text, app):
    text = text or ""
    return (APP_PREFIX + text) if app else text

def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("text", nargs="?")
    p.add_argument("--to")
    p.add_argument("--no-context", action="store_true")
    p.add_argument("--app", action="store_true")
    p.add_argument("--reply-to-ms", type=int)
    p.add_argument("--image")
    p.add_argument("--video")
    p.add_argument("--file")
    p.add_argument("--caption", default="")
    a = p.parse_args(argv)
    media_flags = [("image", a.image), ("video", a.video), ("file", a.file)]
    chosen = [(k, v) for k, v in media_flags if v]
    if len(chosen) > 1 or (not chosen and not a.text):
        print("REFUSED: pass text or exactly one of --image, --video, --file.")
        return 6
    if a.to:
        try:
            if a.to != owner_id():
                print("REFUSED: recipient is not the scan-bound owner.")
                return 6
        except OwnerMissing:
            print("REFUSED: recipient is not the scan-bound owner.")
            return 6
    if chosen and not os.path.isfile(chosen[0][1]):
        print("REFUSED: media file could not be read.")
        return 6
    if a.reply_to_ms is not None:
        last = outbox.last_inbound_s()
        if last and last * 1000 > a.reply_to_ms + 500:
            print("SUPERSEDED: %s sent a newer message; not sending. Stop this run without filing anything." % OWNER_NAME)
            return 4
    context = "" if a.no_context else None
    try:
        if chosen:
            kind, path = chosen[0]
            caption = outgoing_text(a.caption or a.text or "", a.app)
            status, outcome = outbox.send_media(path, kind, caption=caption, context_token=context)
        else:
            text = outgoing_text(a.text, a.app)
            status, outcome = outbox.send_or_queue(text, to=a.to, context_token=context)
    except (OwnerMismatch, OwnerMissing):
        print("REFUSED: recipient is not the scan-bound owner.")
        return 6
    except ValueError:
        print("REFUSED: unsupported send.")
        return 6
    except OSError:
        print("REFUSED: media file could not be read.")
        return 6
    detail = outcome.line() if outcome is not None and hasattr(outcome, "line") else ""
    if status == "sent":
        print("sent" + (": " + detail if detail else ""))
        return 0
    if status == "ambiguous":
        print("AMBIGUOUS (not sent again automatically)" + (": " + detail if detail else ""))
        return 5
    print("QUEUED (will auto-send when the owner next writes)" + (": " + detail if detail else ""))
    return 3

if __name__ == "__main__":
    sys.exit(main())
