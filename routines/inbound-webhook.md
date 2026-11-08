# Routine 模板 ①：微信消息入口（Webhook 触发）

> 在 Grok Bot 里建一个 **routine**，触发方式选 **Webhook**。URL 和 Bearer key 只放进跑转发器的进程环境变量
> `WECHAT_WEBHOOK_URL` / `WECHAT_WEBHOOK_KEY`。不要写进 `.env`、不要提交、不要贴到 issue。
> 程序不读 `.env` 或 secrets 文件里的这两个值。
> 下面是 routine 的指令模板（按需改名字、路径和你自己的整理规则）。
> 原作者实际用的 routine 指令保存在 Grok Bot 的 routine 配置里，不在本仓库；这里是按当前代码契约整理的版本。

---

**指令（贴进 routine）：**

你收到的是微信 ClawBot 转发给绑定者本人的一批消息（JSON，`source = "wechat-clawbot"`, `batch = true`）。
不是绑定者的消息不会出现在这里。

字段：
- `messages[]`：绑定者在一次停顿前连发的消息，按时间排序。每条有 `texts`（文字 / 语音转写）、
  `items[]`（类型、文本或元数据；媒体成功时带 `file` 本地路径，失败带 `media_error`）、
  `files`（已下载并解密的图片 / 文件 / 语音 / 视频绝对路径）、`create_time_ms`、`message_id`。
- `history`：最近 40 行对话。你每次都是新开的一轮，靠它接上文。
- `trigger_ms`：这批里最新一条的时间戳。
- `reply_rule`：回复规则（与下面的退出码一致）。
- `context_note`（可选）：比如“这是对续期提醒的回复”——照它说的简短回应。

规则：
1. 回复只用 `python3 /path/to/grokbot-wechat/send.py`，每次都带 `--reply-to-ms <trigger_ms>`。
   文字：`send.py "回复内容" --reply-to-ms <trigger_ms>`。
   图片 / 视频 / 文件：`send.py --image 路径`、`--video 路径` 或 `--file 路径`，同样带 `--reply-to-ms`。
   可以发多条。不要加 `--to`，除非它就是扫码绑定的那个 id；其它 id 会被拒绝。
2. 退出码 **0**：已发到绑定者的微信。
3. 退出码 **4 = SUPERSEDED**：绑定者在你处理期间又发了新消息。**立刻停止**，什么都不发、不归档，新的一轮会处理。
4. 退出码 **3 = QUEUED**：微信明确拒绝（常见是约 24 小时窗口已关）。内容已进 outbox，绑定者下次说话时自动补发。**不要自己再调 send.py 重试。**
5. 退出码 **5 = AMBIGUOUS**：发送结果不确定（超时、空响应或无法解析）。桥不会自动再发。**不要自动再调 send.py**，否则可能发出两条。不要向用户断言“已经发出”或“一定会补发”。
6. 退出码 **6**：发送前被拒绝（收件人不是绑定者、文件读不了、或类型不支持）。不要换一个 `--to` 再试。
7. 图片 / 文件 / 语音 / 视频：直接打开 `files` 里的路径。`media_error` 含 `rejected-host` 表示地址不在微信 CDN，没有下载，不要自己去请求那个地址。
8. 用户只回了 "1"、"续" 之类，或 `context_note` 说这是续期：回一句很短的“续上了”，别追问。
9. 回复要像微信聊天：短、口语、一条一个意思。主动推送用 `send.py --app "…"`，退出码含义同上。
