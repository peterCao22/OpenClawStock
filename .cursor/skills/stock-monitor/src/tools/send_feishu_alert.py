import sys
import os
import json
import requests
from dotenv import load_dotenv

# Load env from project root
# 适配两种路径结构：本地开发 和 OpenClaw 部署
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__))) # 假设在 src/tools/
if not os.path.exists(os.path.join(project_root, ".env")):
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) # 假设在 .cursor/skills/...

load_dotenv(os.path.join(project_root, ".env"))

def get_tenant_access_token(app_id, app_secret):
    """获取飞书企业自建应用的 Tenant Access Token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    payload = {
        "app_id": app_id,
        "app_secret": app_secret
    }
    try:
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") == 0:
            return data.get("tenant_access_token")
        else:
            print(f"Error getting token: {data}")
            return None
    except Exception as e:
        print(f"Exception getting token: {e}")
        return None

def get_user_id_by_mobile(token, mobile):
    """(可选) 通过手机号查询 User ID"""
    url = "https://open.feishu.cn/open-apis/contact/v3/users/batch_get_id"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {"mobiles": [mobile]}
    try:
        resp = requests.post(url, headers=headers, json=payload)
        data = resp.json()
        if data.get("code") == 0 and data.get("data", {}).get("user_list"):
            return data["data"]["user_list"][0]["user_id"]
    except:
        pass
    return None

def send_feishu_card(title, content, url=None):
    # 1. 优先检查是否配置了 Webhook (兼容旧模式)
    webhook_url = os.getenv("FEISHU_WEBHOOK_URL")
    if webhook_url and webhook_url.startswith("http"):
        # ... (Webhook 发送逻辑保持不变，为了节省篇幅，这里复用旧逻辑)
        # 如果用户只配了 AppID，这段会被跳过
        pass 

    # 2. 检查 App ID / Secret (新模式)
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    receiver_id = os.getenv("FEISHU_RECEIVER_ID")

    if not app_id or not app_secret:
        if webhook_url: 
            # 如果有 webhook 但没 app_id，使用 webhook 发送 (旧逻辑)
            _send_via_webhook(webhook_url, title, content, url)
            return
        else:
            print("Error: 既没有配置 FEISHU_WEBHOOK_URL，也没有配置 FEISHU_APP_ID/SECRET")
            return

    # 3. 获取 Token
    token = get_tenant_access_token(app_id, app_secret)
    if not token:
        return

    # 4. 如果没有接收者 ID，尝试获取（这里为了简单，我们打印提示）
    if not receiver_id:
        print("Warning: FEISHU_RECEIVER_ID 未配置。")
        print("请在 .env 中配置接收消息的 user_id, open_id 或 chat_id。")
        print("你可以通过飞书开发者后台调试工具获取，或者查看 OpenClaw 的日志。")
        return

    # 5. 发送消息
    send_url = "https://open.feishu.cn/open-apis/im/v1/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    # 构造卡片内容
    card_content = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": "blue"
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": content}}
        ]
    }
    
    if url:
        card_content["elements"].append({
            "tag": "action",
            "actions": [{
                "tag": "button",
                "text": {"tag": "plain_text", "content": "查看详情"},
                "url": url,
                "type": "primary"
            }]
        })

    # 确定接收者类型 (默认为 open_id，也可以是 chat_id)
    receive_id_type = os.getenv("FEISHU_RECEIVER_TYPE", "open_id")
    
    params = {"receive_id_type": receive_id_type}
    payload = {
        "receive_id": receiver_id,
        "msg_type": "interactive",
        "content": json.dumps(card_content)
    }

    try:
        resp = requests.post(send_url, params=params, headers=headers, json=payload)
        resp.raise_for_status()
        res_data = resp.json()
        if res_data.get("code") == 0:
            print(f"Message sent successfully to {receiver_id}")
        else:
            print(f"Failed to send message: {res_data}")
    except Exception as e:
        print(f"Exception sending message: {e}")

def _send_via_webhook(webhook_url, title, content, url):
    """兼容旧的 Webhook 模式"""
    card = {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": title}, "template": "blue"},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": content}}]
    }
    if url:
        card["elements"].append({
            "tag": "action",
            "actions": [{"tag": "button", "text": {"tag": "plain_text", "content": "查看详情"}, "url": url, "type": "primary"}]
        })
    
    try:
        requests.post(webhook_url, json={"msg_type": "interactive", "card": card})
        print("Message sent via Webhook")
    except Exception as e:
        print(f"Webhook failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python send_feishu_alert.py 'Title' 'Content' [URL]")
    else:
        title = sys.argv[1]
        content = sys.argv[2]
        url = sys.argv[3] if len(sys.argv) > 3 else None
        send_feishu_card(title, content, url)
