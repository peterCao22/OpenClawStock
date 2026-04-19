import os
import time
import requests
from dotenv import load_dotenv

# 加载项目根目录的 .env 文件
# 假设部署结构:
# workspace/
#   .env
#   src/ (或 scripts/)
#     moma_api_client.py
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

MOMA_API_URL = os.getenv("MOMA_API_URL", "http://api.momaapi.com").rstrip("/")
MOMA_API_KEY = os.getenv("MOMA_API_KEY")
API_SLEEP_SECONDS = float(os.getenv("API_SLEEP_SECONDS", "0.5"))

class MomaApiClient:
    def __init__(self):
        self.base_url = MOMA_API_URL
        self.token = MOMA_API_KEY
    
    def _request(self, endpoint, params=None, max_retries=3, fallback_sleep=5):
        """通用请求封装，带重试机制"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}/{self.token}"
        
        for attempt in range(max_retries):
            # 限流控制，每次请求前等待
            time.sleep(API_SLEEP_SECONDS)
            try:
                # 增加超时时间到 60 秒
                response = requests.get(url, params=params, timeout=180)
                # 处理 HTTP 错误 (例如 429 Too Many Requests, 5xx Server Errors)
                if response.status_code == 429:
                    print(f"API Rate Limited (429) on {url}. Retrying in {fallback_sleep} seconds... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(fallback_sleep)
                    continue
                response.raise_for_status()
                
                data = response.json()
                
                # 有些 API 在触发频率限制时会返回 200 OK，但是返回特定的错误码或错误信息
                # 这里假设如果返回字典且有错误码字段 (根据具体API调整，假设错误在 code 或 msg 字段中)
                if isinstance(data, dict):
                    # 如果有类似限流标志可以在这处理
                    pass
                    
                return data
            except requests.RequestException as e:
                # 如果是 404，直接返回 None，不重试
                if isinstance(e, requests.HTTPError) and e.response.status_code == 404:
                    print(f"API 404 Not Found: {url}")
                    return None
                
                print(f"API Request Failed: {url} | Error: {e} (Attempt {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(fallback_sleep)
                else:
                    return None
        return None
        
    def get_stock_by_category(self, category_code):
        """1-2. 根据指数、行业、概念代码获取相关股票"""
        # API 文档示例: http://api.momaapi.com/hszg/gg/ad_qh/TOKEN
        # 这里的 category_code 应该是类似 'ad_qh' 的字符串
        # 确保 category_code 不包含特殊字符，或者进行 URL 编码
        return self._request(f"hszg/gg/{category_code}")

    def get_category_tree(self):
        """1. 指数、行业、概念树"""
        return self._request("hszg/list")
        
    def get_stock_basic(self, stock_code):
        """2. 股票基础信息"""
        return self._request(f"hsstock/instrument/{stock_code}")
        
    def get_financial_index(self, stock_code, st=None, et=None):
        """3. 财务主要指标"""
        params = {}
        if st: params["st"] = st
        if et: params["et"] = et
        return self._request(f"hsstock/financial/pershareindex/{stock_code}", params=params)

    def get_limit_up_pool(self, date_str):
        """4. 涨停股池 date_str: yyyy-MM-dd"""
        return self._request(f"hslt/ztgc/{date_str}")
        
    def get_limit_down_pool(self, date_str):
        """5. 跌停股池 date_str: yyyy-MM-dd"""
        return self._request(f"hslt/dtgc/{date_str}")
        
    def get_technical_macd(self, stock_code, st=None, et=None):
        """历史分时MACD (仅示例: 日线)"""
        params = {}
        if st: params["st"] = st
        if et: params["et"] = et
        return self._request(f"hsstock/technical/macd/{stock_code}", params=params)

    def get_technical_ma(self, stock_code, st=None, et=None):
        """历史分时MA"""
        params = {}
        if st: params["st"] = st
        if et: params["et"] = et
        return self._request(f"hsstock/technical/ma/{stock_code}", params=params)

    def get_technical_boll(self, stock_code, st=None, et=None):
        """历史分时BOLL"""
        params = {}
        if st: params["st"] = st
        if et: params["et"] = et
        return self._request(f"hsstock/technical/boll/{stock_code}", params=params)

    def get_technical_kdj(self, stock_code, st=None, et=None):
        """历史分时KDJ"""
        params = {}
        if st: params["st"] = st
        if et: params["et"] = et
        return self._request(f"hsstock/technical/kdj/{stock_code}", params=params)

    def get_realtime_quotes(self, stock_codes):
        """7. 实时交易数据（多股）"""
        # stock_codes 可以是逗号分隔的字符串
        if isinstance(stock_codes, list):
            stock_codes = ",".join(stock_codes)
        params = {"stock_codes": stock_codes}
        return self._request("hsrl/ssjy_more", params=params)

    def get_stock_list(self):
        """获取基础的股票代码和名称"""
        return self._request("hslt/list")
