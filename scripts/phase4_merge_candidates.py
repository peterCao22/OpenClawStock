"""
Phase 4A + Phase 4B 候选池合并工具。

将 phase4_weekly_screener（蓄势回踩，默认 top250）与 phase4b_screener（新鲜突破，默认 top100）
的 JSON 结果按股票代码去重，输出统一候选列表，便于导入监控或下游流程。

合并规则（稳定、可预期）：
    1. 先按 4A 文件中的顺序写出每条记录（保留 4A 的名次意义）。
    2. 若某代码同时出现在 4B 中，在同一条记录上补充 4B 的名次与分数，merge_sources 记为 4A+4B。
    3. 仅在 4B 出现的代码，接在列表末尾，顺序与 4B 文件一致。

运行示例：
    python scripts/phase4_merge_candidates.py \\
        --phase4a output/phase4_full_20251231_v4e.json \\
        --phase4b output/phase4b_full_20251231.json \\
        --output output/phase4_merged_20251231.json

注意：
    - 若两个文件的 as_of 不一致，脚本默认告警但仍继续合并（可用 --force 抑制提示）。
    - 4A 与 4B 的 score 口径不同，勿直接横向比较；请使用 score_4a / score_4b。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = ROOT_DIR / "output"


def _load_json(path: Path) -> Dict[str, Any]:
    """
    读取 JSON 文件为字典。

    参数：
        path: 文件路径
    返回：
        解析后的 dict
    异常：
        FileNotFoundError: 文件不存在
        json.JSONDecodeError: 非合法 JSON
    """
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


def merge_pools(
    data_a: Dict[str, Any],
    data_b: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    将 4A、4B 的 candidates 去重合并为统一列表。

    参数：
        data_a: phase4_weekly_screener 输出的根对象（含 candidates）
        data_b: phase4b_screener 输出的根对象（含 candidates）
    返回：
        (merged_candidates, meta) — meta 含数量统计便于写入结果文件
    """
    list_a: List[Dict[str, Any]] = data_a.get("candidates") or []
    list_b: List[Dict[str, Any]] = data_b.get("candidates") or []

    # 4B 代码 -> (名次 1-based, 原始条目)
    b_index: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    for i, row in enumerate(list_b):
        inst = row.get("instrument")
        if inst:
            b_index[inst] = (i + 1, row)

    merged: List[Dict[str, Any]] = []
    overlap_count = 0
    seen_4a: set = set()

    for i, row_a in enumerate(list_a):
        inst = row_a.get("instrument")
        if not inst:
            continue
        seen_4a.add(inst)
        entry: Dict[str, Any] = dict(row_a)
        entry["merge_sources"] = ["4A"]
        entry["rank_4a"] = i + 1
        entry["score_4a"] = row_a.get("score")
        entry["rank_4b"] = None
        entry["score_4b"] = None

        if inst in b_index:
            rank_b, row_b = b_index[inst]
            overlap_count += 1
            entry["merge_sources"] = ["4A", "4B"]
            entry["rank_4b"] = rank_b
            entry["score_4b"] = row_b.get("score")
            # 保留 4B 独有字段到子对象，避免覆盖 4A 同名字段（如 trigger_week 可能一致但语义需区分时）
            entry["detail_4b"] = row_b

        merged.append(entry)

    only_b = 0
    for j, row_b in enumerate(list_b):
        inst = row_b.get("instrument")
        if not inst or inst in seen_4a:
            continue
        only_b += 1
        entry = dict(row_b)
        entry["merge_sources"] = ["4B"]
        entry["rank_4a"] = None
        entry["score_4a"] = None
        entry["rank_4b"] = j + 1
        entry["score_4b"] = row_b.get("score")
        merged.append(entry)

    meta = {
        "count_4a_input": len(list_a),
        "count_4b_input": len(list_b),
        "count_merged": len(merged),
        "count_4a_only": len(list_a) - overlap_count,
        "count_4b_only": only_b,
        "count_overlap": overlap_count,
    }
    return merged, meta


def run_merge(
    phase4a: Path,
    phase4b: Path,
    output: Optional[Path],
    force: bool,
) -> None:
    """
    加载两个 JSON、校验 as_of、合并并写出结果文件。

    参数：
        phase4a: 4A 结果 JSON 路径
        phase4b: 4B 结果 JSON 路径
        output: 输出 JSON 路径；None 时使用 output/phase4_merged_{as_of}.json
        force: True 时 as_of 不一致也不打印警告
    返回：
        None
    """
    data_a = _load_json(phase4a)
    data_b = _load_json(phase4b)

    if output is None:
        as_of_slug = (data_a.get("as_of") or "unknown").replace("-", "")
        output = DEFAULT_OUT_DIR / f"phase4_merged_{as_of_slug}.json"

    as_of_a = data_a.get("as_of")
    as_of_b = data_b.get("as_of")
    if as_of_a != as_of_b and not force:
        print(
            f"[警告] as_of 不一致: 4A={as_of_a!r}  4B={as_of_b!r}，"
            f"合并结果仍写入，但回测/监控口径请自行对齐。",
            file=sys.stderr,
        )

    merged, stats = merge_pools(data_a, data_b)

    payload = {
        "as_of": as_of_a or as_of_b,
        "as_of_4a": as_of_a,
        "as_of_4b": as_of_b,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "Phase4 merged pool (4A + 4B, dedup)",
        "inputs": {
            "phase4a": str(phase4a.resolve()),
            "phase4b": str(phase4b.resolve()),
        },
        "params_4a": data_a.get("params"),
        "params_4b": data_b.get("params"),
        "merge_stats": stats,
        "candidates": merged,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(
        f"已写入 {output} | 合并 {stats['count_merged']} 只 "
        f"(4A={stats['count_4a_input']} 4B={stats['count_4b_input']} "
        f"重叠={stats['count_overlap']} 仅4B={stats['count_4b_only']})"
    )


def main() -> None:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="合并 Phase 4A 与 Phase 4B 候选 JSON，去重输出统一列表"
    )
    parser.add_argument(
        "--phase4a",
        type=Path,
        required=True,
        help="4A JSON（phase4_weekly_screener 输出）",
    )
    parser.add_argument(
        "--phase4b",
        type=Path,
        required=True,
        help="4B JSON（phase4b_screener 输出）",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="输出 JSON；默认 output/phase4_merged_{as_of}.json",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="as_of 不一致时不打印警告",
    )
    args = parser.parse_args()

    run_merge(args.phase4a, args.phase4b, args.output, force=args.force)


if __name__ == "__main__":
    main()
