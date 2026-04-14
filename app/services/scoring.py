import pandas as pd


def build_top_risk_factors(row: pd.Series) -> str:
    factors = []

    if row.get("payment_failure_rate", 0) >= 0.5 and row.get("payments_count", 0) >= 2:
        factors.append("high payment failure rate")

    if row.get("payment_failures_count", 0) >= 3:
        factors.append("repeated failed payments")

    if row.get("complaints_count", 0) >= 2:
        factors.append("multiple complaints")

    if row.get("open_complaints_count", 0) + row.get("escalated_complaints_count", 0) >= 1:
        factors.append("open or escalated complaints")

    if row.get("fraud_exit_count", 0) >= 1:
        factors.append("fraud-related membership exit")

    if row.get("risky_exit_count", 0) >= 2:
        factors.append("unstable membership history")

    if row.get("low_history_flag", 0) == 1:
        factors.append("limited history")

    if row.get("inactive_flag", 0) == 1:
        factors.append("inactive profile")

    return "; ".join(factors[:3]) if factors else "no major risk factor detected"


def score_to_level(score: float) -> str:
    if score >= 70:
        return "critical"
    if score >= 45:
        return "high"
    if score >= 20:
        return "medium"
    return "low"


def compute_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    payment_component = (
        20 * df["payment_failure_rate"].clip(0, 1) +
        2 * df["payment_failures_count"].clip(0, 5) +
        2 * df["payment_disputed_count"].clip(0, 5)
    ).clip(0, 40)

    complaint_component = (
        4 * df["complaints_count"].clip(0, 4) +
        4 * (df["open_complaints_count"] + df["escalated_complaints_count"]).clip(0, 4) +
        2 * df["fraud_suspicion_complaints_count"].clip(0, 3)
    ).clip(0, 25)

    membership_component = (
        3 * df["risky_exit_count"].clip(0, 4) +
        4 * df["fraud_exit_count"].clip(0, 2) +
        2 * df["left_membership_count"].clip(0, 4)
    ).clip(0, 20)

    uncertainty_component = (
        7 * df["low_history_flag"] +
        8 * df["inactive_flag"]
    ).clip(0, 15)

    df["risk_score"] = (
        payment_component +
        complaint_component +
        membership_component +
        uncertainty_component
    ).round(2)

    df["risk_level"] = df["risk_score"].apply(score_to_level)
    df["top_risk_factors"] = df.apply(build_top_risk_factors, axis=1)

    return df.sort_values(
        ["risk_score", "complaints_count", "payment_failures_count"],
        ascending=[False, False, False],
    )