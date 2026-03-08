# 飞书机器人配置指南

目前系统通过**飞书群自定义机器人 Webhook**的方式发送异动告警卡片。
这是一种最轻量、最快捷的单向推送配置方式。

## 配置步骤

1. **创建飞书群组**
   - 在您的飞书中，创建一个用于接收股票监控告警的群组（也可以是只有您自己的群）。
2. **添加自定义机器人**
   - 打开群组设置 -> **群机器人** -> **添加机器人**。
   - 选择 **自定义机器人**，为其命名为 `OpenClaw Stock Monitor`。
3. **获取 Webhook URL**
   - 添加成功后，系统会为您生成一个 Webhook URL。
   - 请复制该 URL。
4. **配置到环境变量**
   - 打开项目根目录下的 `.env` 文件。
   - 添加以下配置项（替换为您刚刚复制的 URL）：
     ```env
     FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
     ```

## 进阶：接收飞书指令 (指令闭环)
如果您希望直接在飞书中回复“买入 000001”并让 OpenClaw 接收到，您需要：
1. 前往 [飞书开放平台](https://open.feishu.cn/) 创建一个 **企业自建应用**。
2. 获取 `App ID` 和 `App Secret`，并在 `.env` 中配置。
3. 在飞书开放平台配置 **事件订阅 (Event Subscription)** 的请求地址（需有一个公网可访问的服务器或内网穿透工具，如 ngrok）。
4. 订阅 `im.message.receive_v1` 事件。
5. 开发一个 Web 服务 (如使用 FastAPI/Flask) 接收该事件的回调，并解析您的自然语言文本。

*注：当前版本 (`scripts/feishu_bot.py`) 已实现了轻量的 Webhook 卡片推送功能。高级交互闭环待 Web 服务启动后集成。*
