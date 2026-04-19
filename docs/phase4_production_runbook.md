# Phase 4 生产环境操作指南

本文说明从本地数据到「合并候选池 → 监控子集 → OpenClaw/异动监控」的**推荐执行顺序**与命令示例。所有脚本均在项目根目录下执行。

---

## 0. 前置条件（一次性或定期维护）

1. **Python 环境**：使用项目约定的 Conda 环境（如 `openclaw_env`），依赖已安装（见 `requirements.txt`）。
2. **配置**：`.env` 中数据库、Moma API、飞书 Webhook 等已按文档配置完成。
3. **数据库**：
   - `kline_qfq`、`stock_list`、`trading_calendar` 已同步；
   - 日 K 需覆盖 Phase 4 所需历史起点（与 `phase4_weekly_screener.py` 中 `DATA_START` 一致，当前为 `2019-01-01` 起至所选 **as-of**）。
4. **飞书**（若使用 `stock_monitor` 异动卡片）：按 `docs/feishu_setup.md` 完成机器人配置。

---

## 1. 确定截面日 `as-of`

- 全链路使用**同一** `YYYY-MM-DD`，例如月末复盘用 `2025-12-31`，当周扫描用最近已完整收盘的交易日。
- **as-of** 表示筛选与特征计算的截止日期（含当日）。

---

## 2. Phase 4A：蓄势回踩型（周线筛选）

```bash
python scripts/phase4_weekly_screener.py --as-of YYYY-MM-DD --top-n 250 --output output/phase4_full_YYYYMMDD.json
```

- `--top-n 250` 与当前默认策略一致；若需全量候选可设 `--top-n 0`（文件会很大，一般仅分析用）。
- 建议输出文件名**带日期**，避免覆盖历史结果。

---

## 3. Phase 4B：新鲜突破型

```bash
python scripts/phase4b_screener.py --as-of YYYY-MM-DD --top-n 100 --output output/phase4b_full_YYYYMMDD.json
```

- 与 4A 互补（触发窗口、评分逻辑不同），**同一 as-of**。

---

## 4. 合并 4A + 4B（统一母池，去重）

```bash
python scripts/phase4_merge_candidates.py ^
  --phase4a output/phase4_full_YYYYMMDD.json ^
  --phase4b output/phase4b_full_YYYYMMDD.json ^
  --output output/phase4_merged_YYYYMMDD.json
```

（Linux / macOS 可将行续接符 `^` 改为 `\`。）

- 默认会检查两路 JSON 的 `as-of` 是否一致；不一致时打印警告。若确需合并且忽略警告，可加 `--force`。
- 典型规模：250 + 100 去重后约 350 只（是否重叠取决于当期数据）。

---

## 5. 导出监控子集（写入 OpenClaw / stock_monitor）

```bash
python scripts/phase4_export_monitoring_targets.py ^
  --merged output/phase4_merged_YYYYMMDD.json ^
  --head 300 ^
  --output results/monitoring_targets.json
```

- **`results/monitoring_targets.json`** 为 `scripts/stock_monitor.py` 与 stock-monitor Skill 约定的监控列表。
- **`--order-mode monitor_trend`（默认）**：**入选集合**与 `score_global` 相同（按模型 score 截断 `--head`），再在名单内重排：优先 **周线多头排列**（MA5>MA10>MA20>MA60 及收盘在 MA60 上方等计分）、**收盘/周线 MA60**、**收盘/18 周均线**（约 **90 个交易日** 位置）、**近 5 个交易日涨幅**（近似最近一周）。依赖合并 JSON 中含 `weekly_ma_alignment_score`、`ret_5d_pct` 等字段，需用当前版本的 4A/4B 扫描与合并流程重新生成；旧文件缺少字段时会退化为主要按 score 排序。
- **`--order-mode score_global`**：整池按 **score** 降序、同分按 **trigger_recency_days** 降序，再取前 `--head` 条。避免旧版「合并文件先 4A 再 4B」导致 4B 高分股被压在名单后半段、难以进入监控截断。
- **`--order-mode merge`**：恢复为合并 JSON 原始顺序（先 4A 后 4B）截断，仅在你需要与旧结果严格对齐时使用。
- **`--head`**：默认 **300**（脚本 `DEFAULT_HEAD`）。可与 **每日全表巡检一次 + 按周覆盖统计** 搭配。Moma 配额不足时可酌减 `--head` 并接受捕获率下降。
- **`--order-mode learned_proxy`**：仅建议 **离线实验**（单次 as-of 拟合），**生产默认勿用**；见 `docs/phase_4.1_optimization_plan.md` §11.4。
- **`--blend-vr-tail` / `--blend-vr-window`**（可选，默认 0）：在分数断点后的窗口内按 **VR** 递补若干只，属实验性质；极端高 VR 常为噪声，需自行样本外验证后再用。

---

## 6. 运行异动监控

```bash
python scripts/stock_monitor.py
```

- 持续轮询（示例）：`python scripts/stock_monitor.py --loop`（注意交易时段频率与 Moma API 配额）。名单较长（默认 300）时，可改为 **每日定时跑一次全表** + **按周复盘** 覆盖统计，以省配额、仍保留对强股的捕捉概率。
- OpenClaw 侧请参考 `.cursor/skills/stock-monitor/SKILL.md`，监控源文件路径与上述 `results/monitoring_targets.json` 一致。

---

## 7.（可选）回测校验

在已有候选 JSON 上对照后续区间收益（具体参数以脚本 `--help` 为准）：

```bash
python scripts/phase4_backtest.py --candidates output/phase4_full_YYYYMMDD.json
```

**合并池**建议直接对 `phase4_merged_*.json` 回测，与监控母池一致；区间可自定（例如往后 1～3 个月）：

```bash
python scripts/phase4_backtest.py --candidates output/phase4_merged_YYYYMMDD.json ^
  --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

### 7.1 模拟：监控名单是否覆盖「回测涨幅前列」

**（1）快速：前 K 名命中几只**（与导出 **head** 一致）

```bash
python scripts/phase4_simulate_monitor_vs_backtest.py ^
  --merged output/phase4_merged_YYYYMMDD.json ^
  --backtest-csv output/phase4_merged_YYYYMMDD_backtest.csv ^
  --head 300 --top-winners 10
```

- K 仅表示**本合并池内**回测收益最高的前 K 只。

**（2）KPI：池内涨幅前 N 只的捕获比例 ≥ 70%**

- 「涨幅大」= 池内 `ret` 降序前 **50** 只（`--gainer-top-n 50`）；目标至少捕获 **35** 只（`--target-capture 0.7`）。
- 求当前排序规则下最小监控条数：`--solve-min-head`。

```bash
python scripts/phase4_simulate_monitor_vs_backtest.py ^
  --merged output/phase4_merged_YYYYMMDD.json ^
  --backtest-csv output/phase4_merged_YYYYMMDD_backtest.csv ^
  --head 300 --gainer-top-n 50 --target-capture 0.7 --solve-min-head
```

- 可加 `--strict-target`：当前 `--head` 未达标则非零退出。
- `--order-mode`、`--blend-vr-tail` 须与导出一致。

---

## 8. 建议节奏

| 频率 | 建议动作 |
|------|----------|
| 按周或按月底 | 同步日 K 与交易日历 → 重复步骤 **1～5**（更新 as-of 与输出文件名）；对 **多个 as-of** 跑步骤 **7.1** 或 `phase4_multi_asof_metrics.py`，避免特征改动只在单截面过拟合 |
| 每个交易日 | 执行步骤 **6** **一次**（全表 300 只扫一遍即可；或由 OpenClaw/计划任务低频调用） |
| 按周 | 汇总本周异动命中与（如有）新合并池回测覆盖，决定是否调整特征或 `--head` |

---

## 端到端流程（一句话）

**补数 → 定 as-of → 4A（top 250）→ 4B（top 100）→ merge → export 监控列表 → `stock_monitor` / OpenClaw。**

---

## 相关文档与脚本

| 说明 | 路径 |
|------|------|
| Phase 4.1 参数与回测结论 | `docs/phase_4.1_optimization_plan.md` |
| Phase 4 总体规划 | `docs/phase_4_plan.md` |
| 飞书配置 | `docs/feishu_setup.md` |
| 4A 筛选 | `scripts/phase4_weekly_screener.py` |
| 4B 筛选 | `scripts/phase4b_screener.py` |
| 合并 | `scripts/phase4_merge_candidates.py` |
| 导出监控 JSON | `scripts/phase4_export_monitoring_targets.py` |
| 监控名单 vs 回测强势股覆盖 | `scripts/phase4_simulate_monitor_vs_backtest.py` |
| 异动监控 | `scripts/stock_monitor.py` |

---

*文档随脚本参数变更时请同步修订（尤其 `DATA_START`、默认 `top-n`、输出路径）。*
