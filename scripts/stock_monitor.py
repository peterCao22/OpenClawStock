import os
import sys
import json
import glob
from openai import OpenAI
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.moma_api_client import MomaApiClient

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

DOUBAO_BASE_URL = os.getenv("DOUBAO_BASE_URL")
DOUBAO_API_KEY = os.getenv("DOUBAO_API_KEY")
DOUBAO_MODEL_ENDPOINT = os.getenv("DOUBAO_MODEL_ENDPOINT")

client = OpenAI(
    api_key=DOUBAO_API_KEY,
    base_url=DOUBAO_BASE_URL,
)

def get_latest_watchlist():
    """获取最新的 Top-20 股票列表"""
    files = glob.glob('results/top_*_stocks_*.json')
    if not files:
        print("未找到监控列表，请先运行 quant_picker.py")
        return []
    latest_file = max(files, key=os.path.getmtime)
    with open(latest_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def analyze_with_llm(stock_info, quote_info):
    """调用 LLM 进行异动分析"""
    prompt = f"""
你是一个资深A股操盘手。该股票在技术面上已经走出了‘大涨-回调-回抽’的洗盘形态。
请结合以下实时的盘面异动，判断今日的盘面异动是否属于主升浪启动，并给出简短的买入建议。

【股票信息】
代码: {stock_info['instrument']}
名称: {stock_info['name']}
技术面得分: {stock_info['total_score']} (前期大涨日期: {stock_info['surge_date']}, 回调见底: {stock_info['bottom_date']})

【实时盘口异动】
现价: {quote_info.get('p')}
涨跌幅: {quote_info.get('pc')}%
量比: {quote_info.get('lb')}
换手率: {quote_info.get('hs')}%

请输出分析结果（200字以内），并在最后明确给出【操作建议】（例如：建议买入/持续观望/放弃）。
如果判断为“建议买入”，请说明理由。
"""
    
    try:
        response = client.chat.completions.create(
            model=DOUBAO_MODEL_ENDPOINT,
            messages=[
                {"role": "system", "content": "你是一个资深A股交易员和量化分析师。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=500
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"LLM 分析失败: {e}"

def run_monitor():
    watchlist = get_latest_watchlist()
    if not watchlist:
        return
        
    print(f"正在监控 {len(watchlist)} 只股票...")
    
    # 提取股票代码（注意去掉后缀 .SZ 或 .SH 以适配部分API）
    # Moma API 的实时多股可能需要纯数字代码或小写前缀
    # 这里我们尝试直接传入 instrument
    codes = [s['instrument'].split('.')[0] for s in watchlist]
    
    api_client = MomaApiClient()
    quotes = api_client.get_realtime_quotes(codes)
    
    if not quotes:
        print("未能获取到实时行情")
        return
        
    # 转换为字典以便查找
    quotes_dict = {str(q.get('dm', '')): q for q in quotes}
    
    for stock in watchlist:
        code_num = stock['instrument'].split('.')[0]
        quote = quotes_dict.get(code_num)
        
        if not quote:
            continue
            
        # 简单异动规则：涨幅>3% 且 量比>1.5
        if quote.get('pc', 0) > 3.0 and quote.get('lb', 0) > 1.5:
            print(f"\n[{stock['name']}] 触发初步异动规则 (涨幅 {quote['pc']}%, 量比 {quote['lb']})")
            print("正在调用大模型深度分析...")
            analysis = analyze_with_llm(stock, quote)
            print(f"--- 分析报告 ---\n{analysis}\n----------------")
            # 将分析报告推送到飞书
            try:
                from scripts.feishu_bot import send_feishu_alert
                send_feishu_alert(
                    stock_name=stock['name'],
                    stock_code=stock['instrument'],
                    analysis_text=analysis,
                    price=quote['p'],
                    pct_chg=quote['pc']
                )
            except Exception as e:
                print(f"推送飞书失败: {e}")

if __name__ == "__main__":
    run_monitor()
