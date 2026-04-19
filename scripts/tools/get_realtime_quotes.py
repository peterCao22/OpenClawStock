import sys
import os
import json

# Add parent directory to path to import moma_api_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from moma_api_client import MomaApiClient

def get_quotes():
    try:
        # 1. Load targets
        targets_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "results", "monitoring_targets.json")
        if not os.path.exists(targets_path):
            print(json.dumps({"error": f"Targets file not found at {targets_path}"}))
            return

        with open(targets_path, "r", encoding="utf-8") as f:
            targets = json.load(f)
        
        codes = [t["code"] for t in targets]
        
        # 2. Fetch data
        client = MomaApiClient()
        # The API expects comma-separated string
        quotes = client.get_realtime_quotes(codes)
        
        if not quotes:
             print(json.dumps({"error": "No data returned from API"}))
             return

        # 3. Merge with target info (concepts, etc)
        # The API returns a list of dicts. We need to map them back to our targets.
        # Assuming API returns list of dicts with 'code' or similar key.
        # Let's just output the raw API response combined with our local metadata for now.
        
        # Create a map of code -> target info
        target_map = {t["code"]: t for t in targets}
        
        enriched_results = []
        
        # Handle different potential return structures (list or dict)
        data_list = quotes if isinstance(quotes, list) else quotes.get('data', [])
        
        for q in data_list:
            # Moma API usually returns 'dm' or 'code' for stock code
            code = q.get('dm') or q.get('code')
            if code and code in target_map:
                combined = {**target_map[code], **q}
                enriched_results.append(combined)
        
        print(json.dumps(enriched_results, ensure_ascii=False, indent=2))

    except Exception as e:
        print(json.dumps({"error": str(e)}))

if __name__ == "__main__":
    get_quotes()
