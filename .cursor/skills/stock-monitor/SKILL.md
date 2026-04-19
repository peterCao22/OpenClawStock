---
name: stock-monitor
description: A comprehensive stock monitoring skill that tracks a watchlist of stocks for real-time anomalies (price surges, volume spikes) and sends alerts via Feishu.
license: None
---

# Stock Monitor Skill

This skill enables OpenClaw to function as an automated trading assistant. It monitors a specific list of stocks (defined in `results/monitoring_targets.json`) and alerts the user when significant market anomalies occur.

## Capabilities

1.  **Real-time Monitoring**: Fetches live quotes for the target watchlist using Moma API.
2.  **Anomaly Detection**: Automatically detects:
    *   Price surges (> 2.5%)
    *   Volume spikes (Volume Ratio > 1.8)
3.  **Instant Alerts**: Sends structured cards to Feishu (Lark) with price, reason, and AI suggestions.

## Usage

### 1. Start Monitoring (Continuous Loop)
Use this command when the user wants to start the background monitoring process. It will run indefinitely (checking every 60s) until stopped.

```bash
python scripts/stock_monitor.py --loop
```

### 2. Check Status Once
Use this command to perform a single check of the current market status and report any immediate anomalies.

```bash
python scripts/stock_monitor.py
```

### 3. Send a Manual Alert
If the user asks to send a test message or a manual alert to Feishu, use the underlying tool directly:

```bash
python scripts/tools/send_feishu_alert.py "Title" "Message Content" "Optional_URL"
```

## Dependencies

This skill relies on the following scripts working together:
*   `scripts/stock_monitor.py`: The main orchestration script.
*   `scripts/moma_api_client.py`: Handles API connectivity.
*   `scripts/tools/send_feishu_alert.py`: Handles message delivery.
*   `results/monitoring_targets.json`: The configuration file containing the watchlist.
