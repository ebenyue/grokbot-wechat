"""Offline tests: no WeChat, no webhook, no network. Run: python3 -m unittest discover -s tests -v"""
import io, json, os, stat, sys, tempfile, unittest, urllib.error, urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

TMP = tempfile.mkdtemp(prefix="bridge-test-")
os.environ["BRIDGE_DATA_DIR"] = TMP
os.environ.setdefault("OWNER_NAME", "Tester")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import forward, outbox, media, history, ilink, bind, config, send  # noqa: E402

OWNER = "owner@im.wechat"
BJ = ZoneInfo("Asia/Shanghai")
def bj(*a): return datetime(*a, tzinfo=BJ).timestamp()
ORIG_POST = ilink.post
ORIG_CDN = media._cdn_exchange
ORIG_DOWNLOAD = media.download

def write_creds(token="bot-token", user=OWNER, base="https://ilinkai.weixin.qq.com"):
    ilink.atomic_json(ilink.CRED, {"baseurl": base, "bot_token": token, "ilink_user_id": user,
                                   "ilink_bot_id": "bot@im.bot"})

class FakeClock:
    def __init__(self, t=1000.0): self.t = t
    def __call__(self): return self.t

class BatcherTest(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(TMP, "pending-test.json"); self.hist = os.path.join(TMP, "hist-test.log")
        for p in (self.path, self.hist):
            if os.path.exists(p): os.remove(p)
        self.sent, self.clock = [], FakeClock()
    def poster(self, body): self.sent.append(body); return (200, "ok")
    def mk(self, **kw): return forward.Batcher(self.path, quiet=3, cap=60, poster=self.poster, clock=self.clock,
                                               history_path=self.hist, **kw)
    def msg(self, text, ms):
        return {"items": [{"type": 1, "text": text}], "texts": [text], "create_time_ms": ms, "message_id": ms}

    def test_quiet_flush_batches_burst(self):
        b = self.mk()
        b.add(self.msg("a", 1)); self.clock.t += 1; b.add(self.msg("b", 2))
        self.assertFalse(b.maybe_flush())
        self.clock.t += 3
        self.assertTrue(b.maybe_flush())
        self.assertEqual(self.sent[0]["count"], 2)
        self.assertEqual(self.sent[0]["trigger_ms"], 2)
        self.assertIn("--reply-to-ms 2", self.sent[0]["reply_rule"])
        self.assertIn("Tester：a b", open(self.hist).read())

    def test_failed_flush_is_kept_and_survives_restart(self):
        b = forward.Batcher(self.path, quiet=3, cap=60, poster=lambda body: None, clock=self.clock, history_path=self.hist)
        b.add(self.msg("x", 5)); self.clock.t += 5
        self.assertFalse(b.maybe_flush())
        self.assertEqual(len(json.load(open(self.path))["messages"]), 1)
        b2 = self.mk(); self.assertEqual(len(b2.msgs), 1)

    def test_bare_one_after_long_silence_gets_context_note(self):
        b = self.mk(); m = self.msg("1", 50 * 3600 * 1000); m["prev_inbound_ms"] = 10 * 3600 * 1000
        b.add(m); self.clock.t += 5; b.maybe_flush()
        self.assertIn("renewing", self.sent[0].get("context_note", ""))

    def test_permanent_webhook_is_held(self):
        calls = []
        def poster(body):
            calls.append(body); return (401, "secret-body")
        b = self.mk(); b.poster = poster
        b.add(self.msg("x", 1)); self.clock.t += 5
        self.assertFalse(b.maybe_flush())
        self.assertFalse(b.maybe_flush())
        self.assertEqual(len(calls), 1)
        self.assertEqual(b.hold, "permanent")
        saved = json.dumps(json.load(open(self.path)))
        self.assertNotIn("secret-body", saved)
        b2 = self.mk(); b2.poster = poster; self.clock.t += 5
        self.assertFalse(b2.maybe_flush())  # a new process may try the held batch once
        self.assertEqual(len(calls), 2)

    def test_transient_webhook_is_capped(self):
        calls = []
        def poster(body):
            calls.append(1); return None
        b = self.mk(transient_cap=2); b.poster = poster
        b.add(self.msg("x", 1)); self.clock.t += 5
        b.maybe_flush(); self.clock.t += 60; b.maybe_flush(); self.clock.t += 60
        self.assertFalse(b.maybe_flush())
        self.assertEqual(len(calls), 2)
        self.assertEqual(b.hold, "transient-budget")

class NudgeTest(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self._orig = outbox.send_or_queue
        def fake(text, to=None, context_token=None, queue_rejection=True):
            self.calls.append(text)
            return "sent", ilink.Outcome("ok")
        outbox.send_or_queue = fake
    def tearDown(self):
        outbox.send_or_queue = self._orig
    def state(self, last):
        ilink.save_state({"last_inbound_ms": int(last * 1000)})
    def test_normal_window_nudges_at_18h(self):
        last = bj(2026, 10, 1, 18, 0); self.state(last)
        self.assertFalse(outbox.maybe_nudge(last + 17 * 3600))
        self.assertTrue(outbox.maybe_nudge(last + 18 * 3600 + 60))
        self.assertFalse(outbox.maybe_nudge(last + 19 * 3600))
        self.assertEqual(len(self.calls), 1)
    def test_quiet_hours_block(self):
        last = bj(2026, 10, 1, 9, 0); self.state(last)
        self.assertFalse(outbox.maybe_nudge(last + 18 * 3600))
    def test_nudges_early_before_quiet_hours(self):
        last = bj(2026, 10, 1, 8, 0); self.state(last)
        self.assertFalse(outbox.maybe_nudge(bj(2026, 10, 1, 21, 0)))
        self.assertTrue(outbox.maybe_nudge(bj(2026, 10, 1, 23, 35)))

class MediaTest(unittest.TestCase):
    def tearDown(self):
        media._cdn_exchange = ORIG_CDN
        media.download = ORIG_DOWNLOAD
        ilink.post = ORIG_POST
    def test_decrypt_roundtrip_and_keys(self):
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        import base64, binascii
        key = bytes(range(16)); data = b"\x89PNG fake image"
        pad = 16 - len(data) % 16; e = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
        enc = e.update(data + bytes([pad]) * pad) + e.finalize()
        self.assertEqual(media.decrypt(enc, key), data)
        self.assertEqual(media.sniff_ext(data), "png")
        self.assertEqual(media.parse_key({"aeskey": key.hex()}), key)
        self.assertEqual(media.parse_key({"media": {"aes_key": base64.b64encode(key).decode()}}), key)
        self.assertEqual(media.parse_key({"media": {"aes_key": base64.b64encode(key.hex().encode()).decode()}}), key)
    def test_summarize_voice_transcript(self):
        s = media.summarize({"type": 3, "voice_item": {"text": "你好", "media": {"aes_key": "x"}}})
        self.assertEqual(s["text"], "你好"); self.assertNotIn("media", s["meta"])
    def test_image_without_key_kept_when_it_is_an_image(self):
        png = b"\x89PNG\r\n\x1a\nrest"
        media.download = lambda url, timeout=60: png
        path = media.fetch_item({"type": 2, "image_item": {"media": {"encrypt_query_param": "q"}}}, TMP)
        self.assertTrue(open(path, "rb").read().startswith(b"\x89PNG"))
        self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
    def test_file_without_key_is_not_saved(self):
        media.download = lambda url, timeout=60: b"not-an-image"
        with self.assertRaises(ValueError):
            media.fetch_item({"type": 4, "file_item": {"media": {"encrypt_query_param": "q"}, "file_name": "a.bin"}}, TMP)
    def test_off_domain_media_is_rejected(self):
        with self.assertRaises(media.MediaHostError):
            media.media_url({"full_url": "https://evil.example/a"})
        with self.assertRaises(media.MediaHostError):
            media.media_url({"full_url": "http://novac2c.cdn.weixin.qq.com/c2c/x"})
        payload = forward.build_payload({"message_id": 3, "item_list": [
            {"type": 2, "image_item": {"media": {"full_url": "https://evil.example/secret"}}}]})
        blob = json.dumps(payload)
        self.assertNotIn("evil.example", blob)
        self.assertNotIn("raw_items", payload)
        self.assertIn("rejected-host", payload["media_error"])

class ProtocolTest(unittest.TestCase):
    def tearDown(self):
        ilink.post = ORIG_POST
        media._cdn_exchange = ORIG_CDN
    def test_domains_and_redirect_refusal(self):
        self.assertEqual(ilink.https_api_base("https://ilinkai.weixin.qq.com"), "https://ilinkai.weixin.qq.com")
        self.assertEqual(ilink.https_cdn("https://novac2c.cdn.weixin.qq.com/c2c/download?q=1"),
                         "https://novac2c.cdn.weixin.qq.com/c2c/download?q=1")
        for bad in ("https://evil.com", "https://ilinkai.weixin.qq.com.evil.com",
                    "https://user@ilinkai.weixin.qq.com", "https://ilinkai.weixin.qq.com:8443/x",
                    "http://ilinkai.weixin.qq.com"):
            with self.assertRaises(ilink.HostRejected):
                ilink.https_api_base(bad)
        self.assertIsNone(bind.accepted_base("evil.com"))
        self.assertIsNone(bind.accepted_base("ilinkai.weixin.qq.com.evil.com"))
        self.assertIsNone(bind.accepted_base("user@ilinkai.weixin.qq.com"))
        self.assertIsNone(bind.accepted_base("ilinkai.weixin.qq.com:8443"))
        self.assertEqual(bind.accepted_base("a.weixin.qq.com"), "https://a.weixin.qq.com")
        req = urllib.request.Request("https://ilinkai.weixin.qq.com/ilink/bot/getupdates")
        with self.assertRaises(urllib.error.HTTPError) as caught:
            ilink._NoRedirect().redirect_request(req, None, 302, "moved", {}, "https://evil.example/steal")
        self.assertEqual(caught.exception.code, 302)
    def test_longpoll_cap_and_message_id(self):
        self.assertEqual(ilink.clamp_longpoll_ms(999999999), 90000)
        self.assertEqual(ilink.clamp_longpoll_ms(0), 35000)
        self.assertEqual(ilink.clamp_longpoll_ms(40000), 40000)
        self.assertEqual(ilink.norm_id(7), "7")
        self.assertEqual(ilink.norm_id("7"), "7")
        self.assertEqual(forward.failure_sleep(1), 2)
        self.assertEqual(forward.failure_sleep(3), 30)
    def test_stale_session_pauses_one_hour_and_clears_when_credentials_change(self):
        self.assertEqual(ilink.updates_status({"errcode": -14, "ret": 0}), "stale")
        write_creds()
        ilink.pause_session(now=1_000)
        self.assertTrue(ilink.session_paused(now=1_000 + 10))
        self.assertFalse(ilink.session_paused(now=1_000 + 3601))
        ilink.pause_session(now=2_000)
        write_creds(token="replaced-token")
        self.assertFalse(ilink.session_paused(now=2_000 + 10))
    def test_classify_send_results(self):
        ok = ilink.classify_response('{"ret":0,"message_id":12,"errmsg":""}')
        self.assertEqual(ok.kind, "ok"); self.assertEqual(ok.message_id, "12")
        self.assertEqual(ilink.classify_response("{}").kind, "ok")
        bad = ilink.classify_response('{"ret":9,"errcode":4,"errmsg":"see https://evil.example/secret"}')
        self.assertEqual(bad.kind, "rejected")
        self.assertNotIn("https://", bad.errmsg); self.assertIn("[url]", bad.errmsg)
        self.assertEqual(ilink.classify_response("").kind, "ambiguous")
        self.assertEqual(ilink.classify_response("nope").kind, "ambiguous")
        self.assertEqual(ilink.classify_response("[1]").kind, "ambiguous")
        self.assertTrue(ilink.classify_response('{"ret":-14}').stale)

class OwnerAndOutboxTest(unittest.TestCase):
    def setUp(self):
        write_creds()
        ilink.save_state({})
        if os.path.exists(outbox.OUTBOX): os.remove(outbox.OUTBOX)
        self.calls = []
        ilink.post = self._post
    def tearDown(self):
        ilink.post = ORIG_POST
        media._cdn_exchange = ORIG_CDN
    def _post(self, endpoint, body, timeout=15):
        self.calls.append((endpoint, body))
        return json.dumps({"ret": 0, "message_id": 42, "errmsg": ""})
    def test_send_uses_owner_and_owner_context(self):
        ilink.save_state({"last_from_user_id": "other@im.wechat",
                          "context_tokens": {OWNER: "ctx-owner", "other@im.wechat": "ctx-other"}})
        outcome = ilink.dispatch_text("hello")
        self.assertEqual(outcome.kind, "ok")
        self.assertEqual(outcome.message_id, "42")
        msg = self.calls[0][1]["msg"]
        self.assertEqual(msg["to_user_id"], OWNER)
        self.assertEqual(msg["context_token"], "ctx-owner")
        self.assertIn("hello", open(history.HISTORY).read())
    def test_foreign_recipient_is_refused(self):
        with self.assertRaises(ilink.OwnerMismatch):
            ilink.dispatch_text("hi", to="other@im.wechat")
        self.assertEqual(self.calls, [])
        self.assertEqual(send.main(["hi", "--to", "other@im.wechat"]), 6)
    def test_definite_refusal_queues_owner_and_flush_keeps_target(self):
        ilink.post = lambda *a, **k: json.dumps({"ret": 9, "errcode": 4, "errmsg": "https://evil.example/x"})
        status, outcome = outbox.send_or_queue("later")
        self.assertEqual(status, "queued")
        self.assertNotIn("https://", outcome.errmsg)
        queued = json.load(open(outbox.OUTBOX))
        self.assertEqual(queued[0]["state"], "queued")
        self.assertEqual(queued[0]["to"], OWNER)
        self.assertEqual(queued[0]["context_token"], "")
        ilink.save_state({"last_from_user_id": "other@im.wechat", "context_tokens": {"other@im.wechat": "nope"}})
        seen = []
        def post(endpoint, body, timeout=15):
            seen.append(body["msg"]); return json.dumps({"ret": 0, "message_id": "7"})
        ilink.post = post
        outbox._pause = lambda s: None
        self.assertEqual(outbox.flush(), 1)
        self.assertEqual(seen[0]["to_user_id"], OWNER)
        self.assertNotIn("context_token", seen[0])
        self.assertEqual(json.load(open(outbox.OUTBOX)), [])
    def test_ambiguous_send_is_not_flushed(self):
        def post(endpoint, body, timeout=15):
            raise TimeoutError()
        ilink.post = post
        status, outcome = outbox.send_or_queue("maybe")
        self.assertEqual(status, "ambiguous")
        item = json.load(open(outbox.OUTBOX))[0]
        self.assertEqual(item["state"], "ambiguous")
        calls = []
        ilink.post = lambda *a, **k: calls.append(1)
        outbox.flush()
        self.assertEqual(calls, [])
        self.assertEqual(json.load(open(outbox.OUTBOX))[0]["state"], "ambiguous")
    def test_inflight_crash_is_not_resent(self):
        json.dump([{"state": "inflight", "text": "maybe", "to": OWNER, "context_token": "tok",
                    "client_id": "c1", "queued_at": 1}], open(outbox.OUTBOX, "w"))
        calls = []
        ilink.post = lambda *a, **k: calls.append(1)
        outbox.flush()
        self.assertEqual(calls, [])
        self.assertEqual(json.load(open(outbox.OUTBOX))[0]["state"], "ambiguous")
    def test_supersede_and_app_prefix(self):
        ilink.save_state({"last_inbound_ms": 5_000})
        self.assertEqual(send.main(["hi", "--reply-to-ms", "1000"]), 4)
        self.assertEqual(self.calls, [])
        self.assertEqual(send.outgoing_text("hi", True), config.APP_PREFIX + "hi")
    def test_nudge_refusal_is_not_queued(self):
        ilink.post = lambda *a, **k: json.dumps({"ret": 1, "errcode": 2, "errmsg": "closed"})
        last = bj(2026, 10, 1, 18, 0)
        ilink.save_state({"last_inbound_ms": int(last * 1000)})
        self.assertFalse(outbox.maybe_nudge(last + 18 * 3600 + 60))
        self.assertFalse(os.path.exists(outbox.OUTBOX) and json.load(open(outbox.OUTBOX)))
        self.assertFalse(outbox.maybe_nudge(last + 19 * 3600))

class InboundTest(unittest.TestCase):
    def setUp(self):
        write_creds()
        ilink.save_state({})
        self.path = os.path.join(TMP, "in-pending.json")
        if os.path.exists(self.path): os.remove(self.path)
        if os.path.exists(forward.SEEN): os.remove(forward.SEEN)
        self.batcher = forward.Batcher(self.path, quiet=3, cap=60, poster=lambda body: (200, ""), clock=FakeClock())
    def msg(self, user, mid, text="hi", **extra):
        m = {"message_type": 1, "from_user_id": user, "message_id": mid, "create_time_ms": 1000,
             "context_token": "tok-" + user, "item_list": [{"type": 1, "text_item": {"text": text}}]}
        m.update(extra); return m
    def test_owner_only_and_cursor_after_pending(self):
        stranger = self.msg("other@im.wechat", 1, text="secret-stranger")
        owner = self.msg(OWNER, 2, text="hello")
        group = self.msg(OWNER, 3, text="group-secret", group_id="g")
        forward.handle_updates({"ret": 0, "get_updates_buf": "CUR", "longpolling_timeout_ms": 999999999,
                                "msgs": [stranger, owner, group]}, self.batcher)
        self.assertEqual(len(self.batcher.msgs), 1)
        self.assertEqual(self.batcher.msgs[0]["texts"], ["hello"])
        st = ilink.load_state()
        self.assertEqual(st["get_updates_buf"], "CUR")
        self.assertEqual(st["last_from_user_id"], OWNER)
        self.assertEqual(st["context_tokens"], {OWNER: "tok-" + OWNER})
        self.assertEqual(st["longpolling_timeout_ms"], 90000)
        blob = json.dumps(st)
        self.assertNotIn("secret-stranger", blob)
        self.assertNotIn("group-secret", blob)
        forward.handle_updates({"ret": 0, "get_updates_buf": "CUR2", "msgs": [self.msg(OWNER, "2", text="again")]}, self.batcher)
        self.assertEqual(len(self.batcher.msgs), 1)
    def test_cursor_not_saved_when_pending_fails(self):
        def boom(payload): raise RuntimeError("disk")
        self.batcher._add_unlocked = boom
        with self.assertRaises(RuntimeError):
            forward.handle_updates({"ret": 0, "get_updates_buf": "CUR",
                                    "msgs": [self.msg(OWNER, 9)]}, self.batcher)
        self.assertNotEqual(ilink.load_state().get("get_updates_buf"), "CUR")
    def test_missing_owner_does_not_accept(self):
        os.remove(ilink.CRED)
        forward.handle_updates({"ret": 0, "get_updates_buf": "CUR", "msgs": [self.msg(OWNER, 1)]}, self.batcher)
        self.assertEqual(self.batcher.msgs, [])
        self.assertNotEqual(ilink.load_state().get("get_updates_buf"), "CUR")

class WebhookSecretTest(unittest.TestCase):
    def test_dotenv_and_secrets_file_are_ignored(self):
        path = os.path.join(TMP, "local.env")
        open(path, "w").write("WECHAT_WEBHOOK_KEY=from-file\nWECHAT_WEBHOOK_URL=https://from-file.example/hook\nNEW_PLAIN=1\n")
        os.environ.pop("WECHAT_WEBHOOK_KEY", None)
        os.environ.pop("WECHAT_WEBHOOK_URL", None)
        os.environ.pop("NEW_PLAIN", None)
        config._load_dotenv(path)
        self.assertEqual(config.webhook_key(), "")
        self.assertEqual(config.webhook_url(), "")
        self.assertEqual(os.environ.get("NEW_PLAIN"), "1")
        secret = os.path.join(TMP, "secrets.json")
        json.dump({"WECHAT_WEBHOOK_KEY": "from-json"}, open(secret, "w"))
        os.environ["WECHAT_SECRETS_FILE"] = secret
        self.assertEqual(config.webhook_key(), "")
        os.environ["WECHAT_WEBHOOK_KEY"] = "from-env"
        self.assertEqual(config.webhook_key(), "from-env")
        os.environ.pop("WECHAT_WEBHOOK_KEY", None)
        os.environ.pop("WECHAT_SECRETS_FILE", None)
    def test_webhook_log_omits_url_key_and_body(self):
        os.environ["WECHAT_WEBHOOK_URL"] = "https://hooks.example.com/private-path"
        os.environ["WECHAT_WEBHOOK_KEY"] = "super-secret-key"
        def boom(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 401, "no", urllib.request.Request("https://hooks.example.com/").headers if False else None,
                                         io.BytesIO(b"super-secret-key https://hooks.example.com/private-path"))
        # HTTPError wants headers with .items in some versions; pass an email.message-like object.
        from email.message import EmailMessage
        headers = EmailMessage()
        def boom2(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 401, "no", headers, io.BytesIO(b"super-secret-key"))
        orig = ilink.OPENER.open
        ilink.OPENER.open = boom2
        try:
            res = forward.post_webhook({"texts": ["hi"]}, tries=3, sleep=lambda s: None)
        finally:
            ilink.OPENER.open = orig
            os.environ.pop("WECHAT_WEBHOOK_URL", None)
            os.environ.pop("WECHAT_WEBHOOK_KEY", None)
        self.assertEqual(res[0], 401)
        log = open(os.path.join(TMP, "forward.log"), encoding="utf-8").read()
        self.assertNotIn("super-secret-key", log)
        self.assertNotIn("private-path", log)
        self.assertNotIn("hooks.example.com", log)

class UploadTest(unittest.TestCase):
    def setUp(self):
        write_creds()
        ilink.save_state({"context_tokens": {OWNER: "ctx"}})
        if os.path.exists(outbox.OUTBOX): os.remove(outbox.OUTBOX)
        self.file = os.path.join(TMP, "pic.png")
        open(self.file, "wb").write(b"\x89PNG\r\n\x1a\npayload")
        self.cdn_calls = []
        self.posts = []
    def tearDown(self):
        ilink.post = ORIG_POST
        media._cdn_exchange = ORIG_CDN
    def _ok_post(self, upload_url="https://novac2c.cdn.weixin.qq.com/c2c/upload?x=1"):
        def post(endpoint, body, timeout=15):
            self.posts.append((endpoint, body))
            if "getuploadurl" in endpoint:
                return json.dumps({"ret": 0, "upload_full_url": upload_url})
            return json.dumps({"ret": 0, "message_id": "55", "errmsg": ""})
        ilink.post = post
    def test_upload_roundtrip_and_send_item(self):
        self._ok_post()
        def exchange(url, cipher, timeout=60):
            self.cdn_calls.append((url, cipher)); return 200, {"x-encrypted-param": "dl-param"}
        media._cdn_exchange = exchange
        outbox._pause = lambda s: None
        status, outcome = outbox.send_media(self.file, "image", caption="cap")
        self.assertEqual(status, "sent")
        self.assertEqual(outcome.message_id, "55")
        upload_body = self.posts[0][1]
        self.assertEqual(upload_body["media_type"], 1)
        self.assertTrue(upload_body["no_need_thumb"])
        self.assertEqual(upload_body["to_user_id"], OWNER)
        key = bytes.fromhex(upload_body["aeskey"])
        self.assertEqual(media.decrypt(self.cdn_calls[0][1], key), open(self.file, "rb").read())
        sent = self.posts[1][1]["msg"]
        self.assertEqual(sent["to_user_id"], OWNER)
        self.assertEqual(sent["context_token"], "ctx")
        image = sent["item_list"][0]["image_item"]
        self.assertEqual(image["media"]["encrypt_query_param"], "dl-param")
        self.assertEqual(image["media"]["aes_key"], media._aes_b64(key))
        self.assertFalse(os.path.exists(outbox.OUTBOX) and json.load(open(outbox.OUTBOX)))
    def test_hostile_upload_url_is_not_posted(self):
        self._ok_post("https://evil.example/upload")
        media._cdn_exchange = lambda *a, **k: self.cdn_calls.append(1) or (200, {})
        status, _ = outbox.send_media(self.file, "file")
        self.assertEqual(status, "ambiguous")
        self.assertEqual(self.cdn_calls, [])
        self.assertEqual(json.load(open(outbox.OUTBOX))[0]["state"], "ambiguous")
        self.assertTrue(all("sendmessage" not in c[0] for c in self.posts))
    def test_cdn_timeout_does_not_send_or_retry(self):
        self._ok_post()
        def exchange(url, cipher, timeout=60):
            self.cdn_calls.append(1); raise TimeoutError()
        media._cdn_exchange = exchange
        status, _ = outbox.send_media(self.file, "video")
        self.assertEqual(status, "ambiguous")
        self.assertEqual(len(self.cdn_calls), 1)
        self.assertTrue(all("sendmessage" not in c[0] for c in self.posts))
    def test_cdn_client_error_does_not_retry(self):
        self._ok_post()
        def exchange(url, cipher, timeout=60):
            self.cdn_calls.append(1); return 403, {}
        media._cdn_exchange = exchange
        status, prepared = media.upload_plaintext(self.file, "image", OWNER)
        self.assertEqual(status, "rejected")
        self.assertIsNone(prepared)
        self.assertEqual(len(self.cdn_calls), 1)

class BindFileTest(unittest.TestCase):
    def test_local_token_and_private_qr_files(self):
        write_creds(token="local-bot-token")
        self.assertEqual(bind.local_tokens(), ["local-bot-token"])
        self.assertEqual(bind.qr_body(bind.local_tokens())["local_token_list"], ["local-bot-token"])
        self.assertTrue(bind.has_local_credentials())
        ilink.atomic_json(ilink.CRED, {"bot_token": "only-token"})
        self.assertFalse(bind.has_local_credentials())
        bind.write_qr_files(TMP, "https://secret.example/qr-value", b"\x89PNG")
        for name in ("qr.png", "qr_url.txt"):
            self.assertEqual(stat.S_IMODE(os.stat(os.path.join(TMP, name)).st_mode), 0o600)
        self.assertIn("qr-value", open(os.path.join(TMP, "qr_url.txt")).read())

class HealthAndDedupTest(unittest.TestCase):
    def test_session_pause_heartbeat_is_not_ok(self):
        write_creds()
        ilink.pause_session(now=5_000)
        line = forward.write_heartbeat(5_010)
        self.assertTrue(line.startswith("paused "))
        self.assertEqual(forward.heartbeat_state(text=line, now=5_010), "paused")
        self.assertEqual(forward.heartbeat_state(text="paused 8600 5010", now=8_000), "paused")
        self.assertEqual(forward.heartbeat_state(text="5010", now=5_020, stale_s=600), "ok")
        self.assertEqual(forward.heartbeat_state(text="1000", now=2_000, stale_s=600), "stale")
        self.assertEqual(forward.heartbeat_state(text="", now=1), "starting")
        ilink._clear_pause()
    def test_seen_ids_are_not_capped(self):
        ids = [str(i) for i in range(10001)]
        merged = forward.merge_seen([], ids)
        self.assertEqual(len(merged), 10001)
        self.assertEqual(merged[0], "0")
        again = forward.merge_seen(merged, ["0", "10001"])
        self.assertEqual(again[0], "0")
        self.assertEqual(len(again), 10002)
        self.assertNotIn("seen evicted", open(os.path.join(TMP, "forward.log"), encoding="utf-8").read())

if __name__ == "__main__":
    unittest.main()
