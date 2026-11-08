# Routine 模板 ②：转发器看门狗（每 15 分钟）

> 在 Grok Bot 里建一个 **routine**，触发方式选 **定时（every 15 minutes）**，在跑 forward.py 的那台机器上执行。
> 也可以不用 routine，改用 cron：`*/15 * * * * /path/to/grokbot-wechat/watchdog.sh >> /path/to/data/watchdog.log 2>&1`
> Webhook URL 和 key 不在这里配置。

---

**指令（贴进 routine）：**

运行 `bash /path/to/grokbot-wechat/watchdog.sh`。它只输出一行：

- `OK pid=…`：进程在轮询，心跳是新的。**什么都不用做，不要发任何微信消息，安静结束。**
- `PAUSED (ilink session -14) pid=…`：进程还在，但 iLink 登录处于文档里的过期暂停（`ret` / `errcode` 为 `-14`，最多约一小时）。
  **这不是健康，也不是 OK。** 不要重启，不要再跑一次 watchdog 去拉起它，不要发微信。暂停结束或换了 `credentials.json` 之后会自己恢复。
- `RESTARTED (原因) pid=…`：转发器先前不在或心跳过期，脚本已经拉起。不用通知用户；如果一天内重启超过 3 次，
  再用 `send.py --app "微信转发器今天重启了 N 次，原因：…"` 提醒一次。退出码仍按 send.py：3 已排队，5 结果不确定就不要自动重发。
- `FAILED (原因)`：拉不起来。看 `forward.out` 和 `forward.log` 末尾找原因（常见：缺依赖、`credentials.json`
  丢了需要重新 `bind.py`）。能修就修好后再跑一次 watchdog.sh；修不了就把原因简短告诉用户。

注意：
- `PAUSED` 时杀掉进程不会清除暂停，下一轮仍会暂停，只是多一次重启。
- 不要同时手动再启一个 `run_forward.sh`，两个轮询器共用一个 getupdates 游标会互相丢掉消息。
- 不要在转发器运行时跑 `poll.py`。
