import hashlib
import json
import math
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

from app.services.ai_logging import init_ai_tables, log_ai_call, read_cache, write_cache


ROOT_DIR = Path(__file__).resolve().parents[2]
PROMPTS_DIR = ROOT_DIR / "prompts"


def _json_safe(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat() if pd.notna(value) else None

    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]

    if isinstance(value, pd.Series):
        return [_json_safe(v) for v in value.tolist()]

    if isinstance(value, pd.DataFrame):
        return [{str(k): _json_safe(v) for k, v in row.items()} for row in value.to_dict(orient="records")]

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    return value

def _stable_json(data: dict[str, Any]) -> str:
    return json.dumps(_json_safe(data), ensure_ascii=False, sort_keys=True)


def _load_prompt(prompt_name: str) -> str:
    prompt_path = PROMPTS_DIR / prompt_name
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt not found: {prompt_path}")
    return prompt_path.read_text(encoding="utf-8")


def _make_cache_key(model: str, prompt_version: str, context: dict[str, Any]) -> str:
    raw = f"{model}|{prompt_version}|{_stable_json(context)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _rough_token_estimate(text: str) -> int:
    return max(1, math.ceil(len(text) / 4))


def _estimate_cost_usd(input_text: str, output_text: str) -> float | None:
    in_price = os.getenv("AI_INPUT_PRICE_PER_1M_TOKENS")
    out_price = os.getenv("AI_OUTPUT_PRICE_PER_1M_TOKENS")

    if not in_price or not out_price:
        return None

    try:
        in_price = float(in_price)
        out_price = float(out_price)
    except ValueError:
        return None

    input_tokens = _rough_token_estimate(input_text)
    output_tokens = _rough_token_estimate(output_text)

    cost = (input_tokens / 1_000_000) * in_price + (output_tokens / 1_000_000) * out_price
    return round(cost, 6)


def _validate_analyst_output(data: dict[str, Any]) -> dict[str, Any]:
    required_keys = [
        "summary",
        "behavior_observed",
        "warning_signals",
        "comparison_to_baseline",
        "decision_support",
        "missing_information",
    ]

    for key in required_keys:
        if key not in data:
            raise ValueError(f"Missing key in analyst output: {key}")

    for list_key in [
        "behavior_observed",
        "warning_signals",
        "comparison_to_baseline",
        "missing_information",
    ]:
        if not isinstance(data[list_key], list):
            raise ValueError(f"Expected list for key: {list_key}")

    return data


def _recent_records(df: pd.DataFrame, sort_col: str, cols: list[str], n: int = 5) -> list[dict[str, Any]]:
    if df.empty:
        return []

    out = df.copy()
    if sort_col in out.columns:
        out = out.sort_values(sort_col, ascending=False)

    keep_cols = [c for c in cols if c in out.columns]
    records = out[keep_cols].head(n).to_dict(orient="records")
    return [_json_safe(r) for r in records]


def _safe_mean(df: pd.DataFrame, col: str) -> float | None:
    if col not in df.columns:
        return None
    series = pd.to_numeric(df[col], errors="coerce")
    if series.dropna().empty:
        return None
    return round(float(series.mean()), 3)


def build_analyst_context(
    selected_row: pd.Series,
    dashboard_df: pd.DataFrame,
    users: pd.DataFrame,
    memberships: pd.DataFrame,
    payments: pd.DataFrame,
    complaints: pd.DataFrame,
) -> dict[str, Any]:
    user_id = int(selected_row["user_id"])

    user_profile = users[users["id"] == user_id].copy()
    user_payments = payments[payments["user_id"] == user_id].copy()
    user_memberships = memberships[memberships["user_id"] == user_id].copy()
    user_complaints = complaints[complaints["target_id"] == user_id].copy()

    risk_percentile = None
    if "risk_score" in dashboard_df.columns and not dashboard_df["risk_score"].dropna().empty:
        risk_percentile = round(
            float((dashboard_df["risk_score"] <= float(selected_row.get("risk_score", 0))).mean() * 100),
            1,
        )

    baseline = {
        "population_size": int(len(dashboard_df)),
        "avg_risk_score": _safe_mean(dashboard_df, "risk_score"),
        "avg_payments_count": _safe_mean(dashboard_df, "payments_count"),
        "avg_payment_failures_count": _safe_mean(dashboard_df, "payment_failures_count"),
        "avg_payment_failure_rate": _safe_mean(dashboard_df, "payment_failure_rate"),
        "avg_complaints_count": _safe_mean(dashboard_df, "complaints_count"),
        "avg_risky_exit_count": _safe_mean(dashboard_df, "risky_exit_count"),
        "risk_score_percentile": risk_percentile,
    }

    profile_record = user_profile.iloc[0].to_dict() if not user_profile.empty else {}
    profile_record = _json_safe(profile_record)

    selected_summary = _json_safe(selected_row.to_dict())

    context = {
        "subscriber": {
            "user_id": user_id,
            "profile": profile_record,
            "scoring_summary": selected_summary,
        },
        "baseline": baseline,
        "recent_payments": _recent_records(
            user_payments,
            sort_col="created_at_parsed" if "created_at_parsed" in user_payments.columns else "created_at",
            cols=[
                "id", "status", "amount_cents", "fee_cents", "currency",
                "stripe_error_code", "created_at", "captured_at"
            ],
            n=5,
        ),
        "recent_memberships": _recent_records(
            user_memberships,
            sort_col="joined_at_parsed" if "joined_at_parsed" in user_memberships.columns else "joined_at",
            cols=["id", "subscription_id", "status", "joined_at", "left_at", "reason"],
            n=5,
        ),
        "recent_complaints": _recent_records(
            user_complaints,
            sort_col="created_at_parsed" if "created_at_parsed" in user_complaints.columns else "created_at",
            cols=["id", "subscription_id", "type", "status", "resolution", "created_at", "resolved_at"],
            n=5,
        ),
    }
    return context


def fallback_analyst_output(context: dict[str, Any]) -> dict[str, Any]:
    scoring = context["subscriber"]["scoring_summary"]
    baseline = context["baseline"]

    behaviors = []
    warnings = []
    comparisons = []
    missing = []

    complaints_count = float(scoring.get("complaints_count", 0) or 0)
    payment_failures_count = float(scoring.get("payment_failures_count", 0) or 0)
    failure_rate = float(scoring.get("payment_failure_rate", 0) or 0)
    risky_exit_count = float(scoring.get("risky_exit_count", 0) or 0)
    payments_count = float(scoring.get("payments_count", 0) or 0)
    risk_score = float(scoring.get("risk_score", 0) or 0)

    if payments_count > 0:
        behaviors.append(f"The subscriber has {int(payments_count)} recorded payments.")
    else:
        behaviors.append("The subscriber has no recorded payment history.")

    if complaints_count > 0:
        behaviors.append(f"The subscriber is the target of {int(complaints_count)} complaint(s).")

    if failure_rate >= 0.5 and payments_count >= 2:
        warnings.append("Payment failure rate is high relative to observed payment history.")

    if payment_failures_count >= 3:
        warnings.append("Repeated failed payments are present.")

    if complaints_count >= 2:
        warnings.append("Multiple complaints are associated with this subscriber.")

    open_plus_escalated = float(scoring.get("open_complaints_count", 0) or 0) + float(scoring.get("escalated_complaints_count", 0) or 0)
    if open_plus_escalated >= 1:
        warnings.append("At least one complaint is still open or escalated.")

    if risky_exit_count >= 1:
        warnings.append("Membership history shows risky exit signals.")

    avg_risk = baseline.get("avg_risk_score")
    if avg_risk is not None:
        if risk_score > avg_risk:
            comparisons.append(f"Risk score ({risk_score}) is above the population average ({avg_risk}).")
        else:
            comparisons.append(f"Risk score ({risk_score}) is not above the population average ({avg_risk}).")

    avg_complaints = baseline.get("avg_complaints_count")
    if avg_complaints is not None and complaints_count > avg_complaints:
        comparisons.append("Complaint volume is above the population average.")

    avg_failures = baseline.get("avg_payment_failures_count")
    if avg_failures is not None and payment_failures_count > avg_failures:
        comparisons.append("Failed payment count is above the population average.")

    if payments_count <= 1:
        missing.append("Very limited payment history reduces confidence.")
    if not context["recent_payments"]:
        missing.append("No recent payment timeline is available.")
    if not context["recent_memberships"]:
        missing.append("No recent membership timeline is available.")

    if not warnings:
        warnings.append("No strong warning signal is visible from the available structured data.")

    summary = (
        "This profile shows "
        f"{'meaningful operational risk' if warnings and warnings[0] != 'No strong warning signal is visible from the available structured data.' else 'limited explicit risk'} "
        "based on payment, complaint, and membership signals."
    )

    decision_support = (
        "This summary suggests whether the profile deserves closer manual review. "
        "Confidence is lower when history is sparse or recent context is missing."
    )

    return {
        "summary": summary,
        "behavior_observed": behaviors[:3],
        "warning_signals": warnings[:3],
        "comparison_to_baseline": comparisons[:3],
        "decision_support": decision_support,
        "missing_information": missing[:3],
    }


def _call_openai_compatible_json(
    system_prompt: str,
    context: dict[str, Any],
) -> tuple[dict[str, Any], str, float | None]:
    api_key = os.getenv("AI_API_KEY")
    base_url = os.getenv("AI_BASE_URL")
    model = os.getenv("AI_MODEL")

    if not api_key or not base_url or not model:
        raise RuntimeError("Missing AI configuration. Expected AI_API_KEY, AI_BASE_URL, AI_MODEL.")

    url = base_url.rstrip("/") + "/chat/completions"

    user_message = _stable_json(context)

    payload = {
        "model": model,
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
    }

    request = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP error from AI provider: {e.read().decode('utf-8', errors='ignore')}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error from AI provider: {e}") from e

    parsed = json.loads(raw)
    content = parsed["choices"][0]["message"]["content"]
    output = json.loads(content)
    output = _validate_analyst_output(output)

    estimated_cost = _estimate_cost_usd(
        input_text=system_prompt + user_message,
        output_text=content,
    )

    return output, model, estimated_cost


def get_analyst_summary(
    selected_row: pd.Series,
    dashboard_df: pd.DataFrame,
    users: pd.DataFrame,
    memberships: pd.DataFrame,
    payments: pd.DataFrame,
    complaints: pd.DataFrame,
) -> dict[str, Any]:
    init_ai_tables()

    prompt_version = "analyst_v1"
    prompt_text = _load_prompt("analyst_v1.md")
    context = build_analyst_context(
        selected_row=selected_row,
        dashboard_df=dashboard_df,
        users=users,
        memberships=memberships,
        payments=payments,
        complaints=complaints,
    )

    configured_model = os.getenv("AI_MODEL", "fallback")
    cache_key = _make_cache_key(configured_model, prompt_version, context)
    user_id = int(selected_row["user_id"])

    cached = read_cache(cache_key)
    if cached is not None:
        cached["source"] = "cache"
        cached["model"] = configured_model
        cached["prompt_version"] = prompt_version
        return cached

    try:
        output, model, estimated_cost = _call_openai_compatible_json(
            system_prompt=prompt_text,
            context=context,
        )
        write_cache(
            cache_key=cache_key,
            user_id=user_id,
            role="analyst",
            model=model,
            prompt_version=prompt_version,
            input_payload=context,
            output_payload=output,
        )
        log_ai_call(
            user_id=user_id,
            role="analyst",
            model=model,
            prompt_version=prompt_version,
            cache_key=cache_key,
            input_payload=context,
            output_payload=output,
            success=True,
            estimated_cost_usd=estimated_cost,
        )
        output["source"] = "llm"
        output["model"] = model
        output["prompt_version"] = prompt_version
        return output
    except Exception as e:
        fallback = fallback_analyst_output(context)
        log_ai_call(
            user_id=user_id,
            role="analyst",
            model=configured_model,
            prompt_version=prompt_version,
            cache_key=cache_key,
            input_payload=context,
            output_payload=fallback,
            success=False,
            error_message=str(e),
            estimated_cost_usd=None,
        )
        fallback["source"] = "fallback"
        fallback["model"] = configured_model
        fallback["prompt_version"] = prompt_version
        return fallback