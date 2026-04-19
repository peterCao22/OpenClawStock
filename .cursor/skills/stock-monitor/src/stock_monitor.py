import os
import sys
import json
import time
from datetime import datetime

# 添加当前目录到路径，以便导入同级模块和子模块
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# 导入工具函数
# 注意：这里假设 tools 文件夹在当前目录下
from tools.get_realtime_quotes import get_quotes
from tools.send_feishu_alert import send_feishu_card
from moma_api_client import MomaApiClient

def load_targets():
    """加载监控目标"""
    try:
        # 假设 targets 文件在 workspace/results/monitoring_targets.json
        # 而本脚本在 workspace/src/stock_monitor.py
        targets_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "results", "monitoring_targets.json")
        with open(targets_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading targets: {e}")
        return []

def get_market_data(codes):
    """获取实时行情"""
    client = MomaApiClient()
    
    # Moma API expects comma-separated string
    quotes = client.get_realtime_quotes(codes)
    
    if not quotes:
        return []
        
    # Handle different potential return structures (list or dict)
    data_list = quotes if isinstance(quotes, list) else quotes.get('data', [])
    return data_list

def check_anomalies(targets, quotes):
    """检查异动"""
    alerts = []
    
    # 创建行情字典
    quotes_map = {}
    for q in quotes:
        code = q.get('dm') or q.get('code')
        if code:
            quotes_map[code] = q
            
    for target in targets:
        code = target['code']
        name = target['name']
        quote = quotes_map.get(code)
        
        if not quote:
            continue
            
        # 获取关键指标
        price = float(quote.get('p', 0) or 0)
        pct_chg = float(quote.get('pc', 0) or 0)  # 涨跌幅 %
        volume_ratio = float(quote.get('lb', 0) or 0) # 量比
        turnover = float(quote.get('hs', 0) or 0) # 换手率
        
        # 触发条件
        # 1. 涨幅 > 2.5%
        # 2. 量比 > 1.8 (放量)
        is_triggered = False
        reasons = []
        
        if pct_chg > 2.5:
            is_triggered = True
            reasons.append(f"股价急涨 (+{pct_chg}%)")
            
        if volume_ratio > 1.8:
            is_triggered = True
            reasons.append(f"量能异常 (量比 {volume_ratio})")
            
        if is_triggered:
            alerts.append({
                "target": target,
                "quote": quote,
                "reasons": reasons,
                "metrics": {
                    "price": price,
                    "pct_chg": pct_chg,
                    "volume_ratio": volume_ratio
                }
            })
            
    return alerts

def format_alert_message(alert):
    """格式化飞书消息"""
    target = alert['target']
    metrics = alert['metrics']
    reasons = alert['reasons']
    concepts = "、".join(target.get('concepts', [])[:3])
    
    title = f"🚀 [异动提醒] {target['name']} ({target['code']})"
    
    content = f"**当前行情**：\n"
    content += f"• 现价：{metrics['price']}\n"
    content += f"• 涨幅：**{metrics['pct_chg']}%**\n"
    content += f"• 量比：{metrics['volume_ratio']}\n\n"
    
    content += f"**触发原因**：\n"
    for r in reasons:
        content += f"• {r}\n"
            
    content += f"\n**核心题材**：{concepts}\n"
    content += f"**AI 建议**：关注板块联动效应，结合大盘情绪操作。"
    
    # 东方财富个股页面 URL
    url = f"http://quote.eastmoney.com/{target['code']}.html"
    if target['code'].startswith('6'):
        url = f"http://quote.eastmoney.com/sh{target['code']}.html"
    else:
        url = f"http://quote.eastmoney.com/sz{target['code']}.html"
        
    return title, content, url

def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始监控轮询...")
    
    # 1. 加载目标
    targets = load_targets()
    if not targets:
        print("未找到监控目标，请检查 results/monitoring_targets.json")
        return

    codes = [t['code'] for t in targets]
    
    # 2. 获取行情
    quotes = get_market_data(codes)
    if not quotes:
        print("未获取到行情数据")
        return
        
    # 3. 检查异动
    alerts = check_anomalies(targets, quotes)
    
    if not alerts:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 暂无异动 (监控中: {len(targets)} 只)")
        return
        
    # 4. 发送报警
    print(f"检测到 {len(alerts)} 个异动，正在发送飞书通知...")
    for alert in alerts:
        title, content, url = format_alert_message(alert)
        send_feishu_card(title, content, url)
        # 避免发送太快
        time.sleep(1)

if __name__ == "__main__":
    # 可以通过命令行参数控制是否循环运行
    # python stock_monitor.py --loop
    
    if "--loop" in sys.argv:
        while True:
            main()
            # 交易时间每 60 秒轮询一次
            time.sleep(60)
    else:
        main()
