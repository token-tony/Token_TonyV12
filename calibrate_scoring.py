"""Calibration utilities for MMS/SSS scoring parameters.

This script ingests historical token data and recommends new configuration
values for the MMS (market health) and SSS (safety) scoring systems.  The
output can be written to stdout or to a file and consumed via the environment
variables ``MMS_SCORING`` and ``SSS_SCORING`` (JSON encoded) before starting
Token Tony.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from config import CONFIG


def _mean(values: Sequence[float]) -> Optional[float]:
    filtered = [v for v in values if v is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def _percentile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    if q <= 0:
        return min(values)
    if q >= 1:
        return max(values)
    data = sorted(values)
    pos = (len(data) - 1) * q
    floor = math.floor(pos)
    ceil = math.ceil(pos)
    if floor == ceil:
        return data[int(pos)]
    lower = data[floor]
    upper = data[ceil]
    return lower + (upper - lower) * (pos - floor)


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: Optional[int] = 0) -> Optional[int]:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _deepcopy(data: Any) -> Any:
    return json.loads(json.dumps(data))


def _load_records(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"No historical data found at {path}")
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            return [dict(row) for row in reader]
    if path.suffix.lower() in {".json", ".jsonl"}:
        text = path.read_text(encoding="utf-8")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, list):
            return [dict(x) for x in payload if isinstance(x, dict)]
        # Assume JSONL fallback
        records: List[Dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                records.append(obj)
        return records
    raise ValueError(f"Unsupported historical data format: {path.suffix}")


def _calibrate_sss(records: Iterable[Dict[str, Any]], target_field: str) -> Dict[str, Any]:
    defaults = _deepcopy(CONFIG.get("SSS_SCORING", {})) or {}
    if not defaults:
        return defaults

    thresholds_raw = list(zip(
        defaults.get("top_holder_thresholds", [80, 60, 40]),
        defaults.get("top_holder_penalties", [40, 25, 10]),
    ))
    try:
        threshold_pairs: List[Tuple[float, float]] = [
            (float(thr), float(pen)) for thr, pen in thresholds_raw
        ]
    except (TypeError, ValueError):
        threshold_pairs = [(80.0, 40.0), (60.0, 25.0), (40.0, 10.0)]
    threshold_pairs.sort(key=lambda x: x[0], reverse=True)

    holder_samples: Dict[float, List[float]] = {thr: [] for thr, _ in threshold_pairs}
    baseline_scores: List[float] = []
    authority_scores: List[float] = []
    rug_scores: List[float] = []
    creator_samples: List[Tuple[int, float]] = []

    baseline_threshold = min([pair[0] for pair in threshold_pairs], default=40.0)
    creator_start = int(defaults.get("creator_penalty_start", 5))

    for record in records:
        raw_target = record.get(target_field)
        target = _safe_float(raw_target, None)
        if target is None:
            continue

        authority = bool(record.get("mint_authority")) or bool(record.get("freeze_authority"))
        pct_val = _safe_float(record.get("top10_holder_percentage"), None)
        rug_score = str(record.get("rugcheck_score", "") or "")
        creator_count = _safe_int(record.get("creator_token_count"), None) or 0

        is_baseline = (
            not authority
            and (pct_val is None or pct_val < baseline_threshold)
            and "high risk" not in rug_score.lower()
            and creator_count <= creator_start
        )
        if is_baseline:
            baseline_scores.append(target)

        if authority:
            authority_scores.append(target)

        if pct_val is not None:
            for thr, _ in threshold_pairs:
                if pct_val >= thr:
                    holder_samples.setdefault(thr, []).append(target)
                    break

        if "high risk" in rug_score.lower():
            rug_scores.append(target)

        if creator_count > creator_start:
            creator_samples.append((creator_count, target))

    baseline_mean = _mean(baseline_scores)
    if baseline_mean is None:
        baseline_mean = float(defaults.get("base_score", 80))
    base_score = max(0.0, min(100.0, baseline_mean))

    authority_mean = _mean(authority_scores)
    if authority_mean is not None:
        authority_penalty = max(0.0, base_score - authority_mean)
    else:
        authority_penalty = float(defaults.get("authority_penalty", 60))

    updated_penalties: List[float] = []
    for thr, default_penalty in threshold_pairs:
        samples = holder_samples.get(thr) or []
        sample_mean = _mean(samples)
        if sample_mean is not None:
            updated_penalties.append(max(0.0, base_score - sample_mean))
        else:
            updated_penalties.append(float(default_penalty))

    rug_mean = _mean(rug_scores)
    if rug_mean is not None:
        rug_penalty = max(0.0, base_score - rug_mean)
    else:
        rug_penalty = float(defaults.get("rug_high_risk_penalty", 30))

    creator_penalty = float(defaults.get("creator_penalty_per_token", 3))
    if creator_samples:
        per_token_deltas: List[float] = []
        for count, sample_score in creator_samples:
            extra = max(1, count - creator_start)
            drop = base_score - sample_score
            if drop > 0:
                per_token_deltas.append(drop / extra)
        delta_mean = _mean(per_token_deltas)
        if delta_mean is not None:
            creator_cap = float(defaults.get("creator_penalty_cap", 25))
            creator_penalty = min(creator_cap, max(0.0, delta_mean))

    # Compose the final structure preserving order from defaults
    thresholds_sorted = [int(round(thr)) for thr, _ in threshold_pairs]
    penalties_sorted = [round(pen, 2) for pen in updated_penalties]

    updated = _deepcopy(defaults)
    updated["base_score"] = int(round(base_score))
    updated["authority_penalty"] = int(round(authority_penalty))
    updated["top_holder_thresholds"] = thresholds_sorted
    updated["top_holder_penalties"] = [int(round(p)) for p in penalties_sorted]
    updated["rug_high_risk_penalty"] = int(round(rug_penalty))
    updated["creator_penalty_per_token"] = round(creator_penalty, 2)

    return updated


def _assign_bracket(age: float, brackets: Sequence[Dict[str, Any]]) -> int:
    for idx, bracket in enumerate(brackets):
        max_age = bracket.get("max_age_minutes")
        try:
            limit = float(max_age) if max_age is not None else None
        except (TypeError, ValueError):
            limit = None
        if limit is None or age < limit:
            return idx
    return len(brackets) - 1 if brackets else -1


def _correlation(xs: Sequence[float], ys: Sequence[float]) -> float:
    if len(xs) != len(ys) or len(xs) < 2:
        return 0.0
    mean_x = _mean(xs)
    mean_y = _mean(ys)
    if mean_x is None or mean_y is None:
        return 0.0
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return 0.0
    return num / (den_x * den_y)


def _calibrate_mms(
    records: Iterable[Dict[str, Any]],
    target_field: str,
    performance_threshold: float,
    quantile: float,
) -> Dict[str, Any]:
    defaults = _deepcopy(CONFIG.get("MMS_SCORING", {})) or {}
    brackets = defaults.get("age_brackets")
    if not defaults or not brackets:
        return defaults

    bucketed: Dict[int, List[Dict[str, Any]]] = {idx: [] for idx in range(len(brackets))}
    for record in records:
        age_val = _safe_float(record.get("age_minutes"), None)
        if age_val is None:
            continue
        idx = _assign_bracket(age_val, brackets)
        if idx >= 0:
            bucketed.setdefault(idx, []).append(record)

    updated_brackets: List[Dict[str, Any]] = []
    for idx, bracket in enumerate(brackets):
        bucket = bucketed.get(idx, [])
        if not bucket:
            updated_brackets.append(_deepcopy(bracket))
            continue

        # Separate performance bands
        perf_candidates = []
        high_perf = []
        for record in bucket:
            target = _safe_float(record.get(target_field), None)
            if target is None:
                continue
            perf_candidates.append((record, target))
            if target >= performance_threshold:
                high_perf.append((record, target))
        if len(high_perf) < 3:
            high_perf = perf_candidates
        if not perf_candidates:
            updated_brackets.append(_deepcopy(bracket))
            continue

        def _collect(series: List[Tuple[Dict[str, Any], float]], key: str) -> List[float]:
            out: List[float] = []
            for rec, _ in series:
                val = _safe_float(rec.get(key), None)
                if val is not None and val >= 0:
                    out.append(val)
            return out

        liq_values = _collect(high_perf, "liquidity_usd")
        vol_values = _collect(high_perf, "volume_24h_usd")
        mc_values = _collect(high_perf, "market_cap_usd")

        updated_norms = {
            "liquidity": _percentile(liq_values, quantile)
            or bracket.get("norms", {}).get("liquidity", 5_000),
            "volume": _percentile(vol_values, quantile)
            or bracket.get("norms", {}).get("volume", 25_000),
            "market_cap": _percentile(mc_values, quantile)
            or bracket.get("norms", {}).get("market_cap", 50_000),
        }

        # Correlation-driven weights
        liq_series = []
        vol_series = []
        mc_series = []
        target_series = []
        for rec, target in perf_candidates:
            liq_val = _safe_float(rec.get("liquidity_usd"), None)
            vol_val = _safe_float(rec.get("volume_24h_usd"), None)
            mc_val = _safe_float(rec.get("market_cap_usd"), None)
            if None in (liq_val, vol_val, mc_val):
                continue
            liq_series.append(max(0.0, liq_val))
            vol_series.append(max(0.0, vol_val))
            mc_series.append(max(0.0, mc_val))
            target_series.append(max(0.0, target))

        default_weights = bracket.get("weights", {})
        total_default_weight = sum(
            float(default_weights.get(key, 0)) for key in ("liquidity", "volume", "market_cap")
        )
        if total_default_weight <= 0:
            total_default_weight = 1.0

        corrs = [
            abs(_correlation(series, target_series))
            for series in (liq_series, vol_series, mc_series)
        ]
        if sum(corrs) > 0:
            scale = total_default_weight / sum(corrs)
            new_weights = [c * scale for c in corrs]
        else:
            new_weights = [float(default_weights.get(key, 0)) for key in ("liquidity", "volume", "market_cap")]

        updated_bracket = _deepcopy(bracket)
        updated_bracket.setdefault("weights", {})
        updated_bracket.setdefault("norms", {})
        for metric, value in updated_norms.items():
            if value is None or value <= 0:
                continue
            updated_bracket["norms"][metric] = round(float(value), 2)
        for metric, weight in zip(("liquidity", "volume", "market_cap"), new_weights):
            updated_bracket["weights"][metric] = round(float(weight), 4)

        updated_brackets.append(updated_bracket)

    updated = _deepcopy(defaults)
    updated["age_brackets"] = updated_brackets
    return updated


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Calibrate MMS/SSS scoring parameters")
    parser.add_argument("input", help="Historical dataset (CSV, JSON or JSONL)")
    parser.add_argument("--output", help="Optional path to write the recommended configuration JSON")
    parser.add_argument(
        "--sss-target-field",
        default="observed_sss",
        help="Field containing the realised SSS score (default: observed_sss)",
    )
    parser.add_argument(
        "--mms-target-field",
        default="observed_mms",
        help="Field containing the realised MMS score (default: observed_mms)",
    )
    parser.add_argument(
        "--performance-threshold",
        type=float,
        default=70.0,
        help="Minimum realised MMS score treated as high performing",
    )
    parser.add_argument(
        "--quantile",
        type=float,
        default=0.75,
        help="Quantile used for normalisation constants (default: 0.75)",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    dataset_path = Path(args.input)
    records = _load_records(dataset_path)
    if not records:
        raise SystemExit("No usable historical records found for calibration")

    sss_config = _calibrate_sss(records, args.sss_target_field)
    mms_config = _calibrate_mms(records, args.mms_target_field, args.performance_threshold, args.quantile)

    output = {"SSS_SCORING": sss_config, "MMS_SCORING": mms_config}
    payload = json.dumps(output, indent=2, sort_keys=True)

    if args.output:
        output_path = Path(args.output)
        output_path.write_text(payload + "\n", encoding="utf-8")
        print(f"Wrote calibrated configuration to {output_path}")
    else:
        print(payload)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
