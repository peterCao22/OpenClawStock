import os
import json
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

FEISHU_WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL")

def send_feishu_alert(stock_name, stock_code, analysis_text, price, pct_chg):
    """
    通过飞书 Webhook 发送监控告警卡片
    需要在 .env 中配置 FEISHU_WEBHOOK_URL
    """
    if not FEISHU_WEBHOOK_URL:
        print("未配置 FEISHU_WEBHOOK_URL，无法发送飞书消息。")
        return

    # 构造飞书卡片消息
    card_content = {
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"📈 股票异动告警: {stock_name} ({stock_code})"
                },
                "template": "red"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**当前价格:** {price}\n**涨跌幅:** {pct_chg}%\n\n**🤖 LLM 分析报告:**\n{analysis_text}"
                    }
                },
                {
                    "tag": "hr"
                },
                {
                    "tag": "note",
                    "elements": [
                        {
                            "tag": "plain_text",
                            "content": "请自行打开券商APP确认并操作。操作后可在聊天中回复 OpenClaw 记录持仓。"
                        }
                    ]
                }
            ]
        }
    }

    try:
        response = requests.post(
            FEISHU_WEBHOOK_URL, 
            headers={"Content-Type": "application/json"},
            data=json.dumps(card_content)
        )
        response.raise_for_status()
        print("飞书告警消息发送成功！")
    except Exception as e:
        print(f"飞书告警消息发送失败: {e}")

if __name__ == "__main__":
    # 测试发送
    send_feishu_alert(
        stock_name="测试股票",
        stock_code="000001",
        analysis_text="该股今日放量突破，符合买入特征，建议买入。",
        price="15.20",
        pct_chg="5.6"
    )
