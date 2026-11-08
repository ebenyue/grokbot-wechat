#!/usr/bin/env python3
"""WeChat ClawBot binding via iLink (method from chihao.li/blog/en/wechat-clawbot-ilink-protocol).
Fetches QR -> writes qr.png and qr_url.txt mode 0600 -> long-polls status -> saves credentials.json on confirm.
The QR URL is not printed. Scan qr.png with the WeChat account that has the ClawBot plugin enabled.
Auto-refreshes QR on expiry. If the phone shows a number (need_verifycode), write it into verify_code.txt.
IDC redirect hosts must be https names under weixin.qq.com. Redirect responses are not followed blindly."""
import json, os, sys, time, urllib.request, urllib.parse, datetime
from config import DATA_DIR as DIR
import ilink

BASE = "https://ilinkai.weixin.qq.com"
HDRS = {"iLink-App-Id": "bot", "iLink-App-ClientVersion": str((2 << 16) | (4 << 8) | 9),
        "Content-Type": "application/json"}
MAX_REFRESH = int(os.environ.get("MAX_REFRESH", "10"))

def log(*a):
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)

def local_tokens(path=None):
    path = path or ilink.CRED
    try:
        with open(path, encoding="utf-8") as f: d = json.load(f)
    except Exception:
        return []
    if not isinstance(d, dict): return []
    tok = str(d.get("bot_token") or "").strip()
    return [tok] if tok else []

def qr_body(tokens):
    return {"local_token_list": list(tokens)[:10]}

def has_local_credentials(path=None):
    path = path or ilink.CRED
    try:
        with open(path, encoding="utf-8") as f: d = json.load(f)
    except Exception:
        return False
    if not isinstance(d, dict): return False
    return bool(str(d.get("bot_token") or "").strip() and str(d.get("ilink_user_id") or "").strip())

def accepted_base(host):
    host = (host or "").strip()
    if not host or any(c in host for c in "/?#@\\ ") or ":" in host: return None
    try:
        return ilink.https_api_base("https://" + host)
    except ilink.HostRejected:
        return None

def write_qr_files(directory, url, png_bytes):
    os.makedirs(directory, exist_ok=True)
    tmp = os.path.join(directory, "qr.tmp.png")
    png = os.path.join(directory, "qr.png")
    txt = os.path.join(directory, "qr_url.txt")
    with open(tmp, "wb") as f: f.write(png_bytes)
    os.chmod(tmp, 0o600)
    os.replace(tmp, png)
    os.chmod(png, 0o600)
    with open(txt, "w", encoding="utf-8") as f: f.write(url + "\n")
    os.chmod(txt, 0o600)

def req(url, data=None, timeout=40):
    r = urllib.request.Request(url, data=data, headers=HDRS, method="POST" if data else "GET")
    with ilink.OPENER.open(r, timeout=timeout) as f:
        return json.loads(f.read().decode())

def new_qr():
    import qrcode
    body = qr_body(local_tokens())
    j = req(f"{BASE}/ilink/bot/get_bot_qrcode?bot_type=3", json.dumps(body).encode())
    url = j["qrcode_img_content"]
    q = qrcode.QRCode(box_size=10, border=4, error_correction=qrcode.constants.ERROR_CORRECT_M)
    q.add_data(url); q.make(fit=True)
    import io
    buf = io.BytesIO(); q.make_image().save(buf, format="PNG")
    write_qr_files(DIR, url, buf.getvalue())
    log("NEW QR saved")
    return j["qrcode"]

def main():
    qr = new_qr(); base = BASE; refreshes = 0; verify = None; last = None
    while True:
        ep = f"{base}/ilink/bot/get_qrcode_status?qrcode={urllib.parse.quote(qr)}"
        if verify: ep += "&verify_code=" + urllib.parse.quote(verify)
        try:
            s = req(ep, timeout=40)
        except Exception as e:
            log("status poll error type=%s" % type(e).__name__)
            s = {"status": "wait"}
        st = s.get("status")
        if st != last: log("status:", st); last = st
        if st == "confirmed":
            path = os.path.join(DIR, "credentials.json")
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f: json.dump(s, f, indent=2)
            os.chmod(tmp, 0o600); os.replace(tmp, path)
            log("BOUND OK saved credentials"); return 0
        if st == "binded_redirect":
            if has_local_credentials():
                log("Already bound"); return 0
            log("binded_redirect without local credentials"); return 1
        if st == "scaned_but_redirect":
            nxt = accepted_base(s.get("redirect_host"))
            if nxt:
                base = nxt; log("redirect accepted")
            else:
                log("redirect rejected")
        if st == "scaned": verify = None
        if st == "need_verifycode":
            vf = os.path.join(DIR, "verify_code.txt")
            log("Phone shows a number: write it to the verify file")
            while not (os.path.exists(vf) and open(vf).read().strip()): time.sleep(2)
            verify = open(vf).read().strip(); os.remove(vf); continue
        if st in ("expired", "verify_code_blocked"):
            refreshes += 1
            if refreshes > MAX_REFRESH: log("too many refreshes, giving up"); return 1
            qr = new_qr(); base = BASE; verify = None
        time.sleep(1)

if __name__ == "__main__":
    sys.exit(main())
