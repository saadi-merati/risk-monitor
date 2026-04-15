from __future__ import annotations

from typing import Any

import pandas as pd


def _ensure_datetime(df: pd.DataFrame, col: str, parsed_col: str) -> pd.DataFrame:
    out = df.copy()
    if parsed_col not in out.columns:
        out[parsed_col] = pd.to_datetime(out[col], errors="coerce", utc=True)
    return out


def _cluster_by_time_gap(df: pd.DataFrame, time_col: str, max_gap_minutes: int) -> pd.DataFrame:
    out = df.sort_values(time_col).copy()
    gaps = out[time_col].diff().dt.total_seconds().div(60)
    new_cluster = gaps.isna() | (gaps > max_gap_minutes)
    out["_cluster_id"] = new_cluster.cumsum()
    return out


def _unique_list(series: pd.Series) -> list[Any]:
    values = []
    for v in series.dropna().tolist():
        if v not in values:
            values.append(v)
    return values


def _score_cluster(size: int, duration_minutes: float, extra: float = 0.0) -> float:
    duration_bonus = max(0.0, 30.0 - min(duration_minutes, 30.0)) / 3.0
    return round(size * 8 + duration_bonus + extra, 2)


def detect_owner_join_bursts(
    subscriptions: pd.DataFrame,
    memberships: pd.DataFrame,
    min_users: int = 4,
    max_gap_minutes: int = 30,
) -> pd.DataFrame:
    m = _ensure_datetime(memberships, "joined_at", "joined_at_parsed")
    s = subscriptions[["id", "owner_id", "brand"]].rename(columns={"id": "subscription_id"}).copy()

    joined = m.merge(s, on="subscription_id", how="left")
    joined = joined[joined["joined_at_parsed"].notna() & joined["owner_id"].notna()].copy()

    if joined.empty:
        return pd.DataFrame()

    clusters = []

    for owner_id, group in joined.groupby("owner_id"):
        group = _cluster_by_time_gap(group, "joined_at_parsed", max_gap_minutes)

        for cluster_id, sub in group.groupby("_cluster_id"):
            distinct_users = sub["user_id"].nunique()
            if distinct_users < min_users:
                continue

            start_at = sub["joined_at_parsed"].min()
            end_at = sub["joined_at_parsed"].max()
            duration_minutes = max(0.0, (end_at - start_at).total_seconds() / 60)

            clusters.append({
                "pattern_id": f"owner_join_burst_{int(owner_id)}_{start_at.strftime('%Y%m%d%H%M%S')}",
                "pattern_type": "owner_join_burst",
                "label": "Several subscribers joined the same owner within a short time window",
                "owner_id": int(owner_id),
                "subscription_ids": _unique_list(sub["subscription_id"]),
                "user_ids": _unique_list(sub["user_id"]),
                "brands": _unique_list(sub["brand"]),
                "affected_users": int(distinct_users),
                "start_at": start_at,
                "end_at": end_at,
                "duration_minutes": round(duration_minutes, 2),
                "evidence": f"{distinct_users} subscribers joined subscriptions from owner {int(owner_id)} within {round(duration_minutes, 2)} minutes.",
                "suspicious_score": _score_cluster(distinct_users, duration_minutes, extra=8),
            })

    return pd.DataFrame(clusters)


def detect_subscription_join_bursts(
    subscriptions: pd.DataFrame,
    memberships: pd.DataFrame,
    min_users: int = 3,
    max_gap_minutes: int = 20,
) -> pd.DataFrame:
    m = _ensure_datetime(memberships, "joined_at", "joined_at_parsed")
    s = subscriptions[["id", "owner_id", "brand"]].rename(columns={"id": "subscription_id"}).copy()

    joined = m.merge(s, on="subscription_id", how="left")
    joined = joined[joined["joined_at_parsed"].notna() & joined["subscription_id"].notna()].copy()

    if joined.empty:
        return pd.DataFrame()

    clusters = []

    for subscription_id, group in joined.groupby("subscription_id"):
        group = _cluster_by_time_gap(group, "joined_at_parsed", max_gap_minutes)

        for cluster_id, sub in group.groupby("_cluster_id"):
            distinct_users = sub["user_id"].nunique()
            if distinct_users < min_users:
                continue

            start_at = sub["joined_at_parsed"].min()
            end_at = sub["joined_at_parsed"].max()
            duration_minutes = max(0.0, (end_at - start_at).total_seconds() / 60)

            owner_id = sub["owner_id"].dropna().iloc[0] if sub["owner_id"].notna().any() else None

            clusters.append({
                "pattern_id": f"subscription_join_burst_{int(subscription_id)}_{start_at.strftime('%Y%m%d%H%M%S')}",
                "pattern_type": "subscription_join_burst",
                "label": "Several subscribers joined the same subscription within a short time window",
                "owner_id": int(owner_id) if owner_id is not None else None,
                "subscription_ids": [int(subscription_id)],
                "user_ids": _unique_list(sub["user_id"]),
                "brands": _unique_list(sub["brand"]),
                "affected_users": int(distinct_users),
                "start_at": start_at,
                "end_at": end_at,
                "duration_minutes": round(duration_minutes, 2),
                "evidence": f"{distinct_users} subscribers joined subscription {int(subscription_id)} within {round(duration_minutes, 2)} minutes.",
                "suspicious_score": _score_cluster(distinct_users, duration_minutes, extra=6),
            })

    return pd.DataFrame(clusters)


def detect_failed_payment_bursts(
    subscriptions: pd.DataFrame,
    payments: pd.DataFrame,
    min_users: int = 3,
    max_gap_minutes: int = 120,
) -> pd.DataFrame:
    p = _ensure_datetime(payments, "created_at", "created_at_parsed")
    p = p.copy()
    p["status_clean"] = p["status"].fillna("").astype(str).str.strip().str.lower()
    p["error_code_clean"] = p["stripe_error_code"].fillna("").astype(str).str.strip().str.lower()

    failed = p[
        p["created_at_parsed"].notna()
        & p["status_clean"].isin(["failed"])
        & (p["error_code_clean"] != "")
    ].copy()

    s = subscriptions[["id", "owner_id", "brand"]].rename(columns={"id": "subscription_id"}).copy()
    failed = failed.merge(s, on="subscription_id", how="left")

    if failed.empty:
        return pd.DataFrame()

    clusters = []

    group_cols = ["owner_id", "error_code_clean"]
    for (owner_id, error_code), group in failed.groupby(group_cols, dropna=True):
        group = _cluster_by_time_gap(group, "created_at_parsed", max_gap_minutes)

        for cluster_id, sub in group.groupby("_cluster_id"):
            distinct_users = sub["user_id"].nunique()
            if distinct_users < min_users:
                continue

            start_at = sub["created_at_parsed"].min()
            end_at = sub["created_at_parsed"].max()
            duration_minutes = max(0.0, (end_at - start_at).total_seconds() / 60)

            clusters.append({
                "pattern_id": f"failed_payment_burst_{int(owner_id)}_{error_code}_{start_at.strftime('%Y%m%d%H%M%S')}",
                "pattern_type": "failed_payment_burst",
                "label": "Multiple subscribers hit the same failed payment code under the same owner",
                "owner_id": int(owner_id),
                "subscription_ids": _unique_list(sub["subscription_id"]),
                "user_ids": _unique_list(sub["user_id"]),
                "brands": _unique_list(sub["brand"]),
                "affected_users": int(distinct_users),
                "start_at": start_at,
                "end_at": end_at,
                "duration_minutes": round(duration_minutes, 2),
                "trigger_value": error_code,
                "evidence": f"{distinct_users} subscribers hit payment error '{error_code}' under owner {int(owner_id)} within {round(duration_minutes, 2)} minutes.",
                "suspicious_score": _score_cluster(distinct_users, duration_minutes, extra=10),
            })

    return pd.DataFrame(clusters)


def detect_complaint_bursts(
    subscriptions: pd.DataFrame,
    complaints: pd.DataFrame,
    min_targets: int = 3,
    max_gap_minutes: int = 1440,
) -> pd.DataFrame:
    c = _ensure_datetime(complaints, "created_at", "created_at_parsed")
    c = c.copy()
    c["type_clean"] = c["type"].fillna("").astype(str).str.strip().str.lower()

    s = subscriptions[["id", "owner_id", "brand"]].rename(columns={"id": "subscription_id"}).copy()
    c = c.merge(s, on="subscription_id", how="left")
    c = c[c["created_at_parsed"].notna() & c["owner_id"].notna() & (c["type_clean"] != "")].copy()

    if c.empty:
        return pd.DataFrame()

    clusters = []

    for (owner_id, type_clean), group in c.groupby(["owner_id", "type_clean"], dropna=True):
        group = _cluster_by_time_gap(group, "created_at_parsed", max_gap_minutes)

        for cluster_id, sub in group.groupby("_cluster_id"):
            distinct_targets = sub["target_id"].nunique()
            if distinct_targets < min_targets:
                continue

            start_at = sub["created_at_parsed"].min()
            end_at = sub["created_at_parsed"].max()
            duration_minutes = max(0.0, (end_at - start_at).total_seconds() / 60)

            clusters.append({
                "pattern_id": f"complaint_burst_{int(owner_id)}_{type_clean}_{start_at.strftime('%Y%m%d%H%M%S')}",
                "pattern_type": "complaint_burst",
                "label": "Similar complaints accumulated quickly under the same owner",
                "owner_id": int(owner_id),
                "subscription_ids": _unique_list(sub["subscription_id"]),
                "user_ids": _unique_list(sub["target_id"]),
                "brands": _unique_list(sub["brand"]),
                "affected_users": int(distinct_targets),
                "start_at": start_at,
                "end_at": end_at,
                "duration_minutes": round(duration_minutes, 2),
                "trigger_value": type_clean,
                "evidence": f"{distinct_targets} targets generated complaint type '{type_clean}' under owner {int(owner_id)} within {round(duration_minutes, 2)} minutes.",
                "suspicious_score": _score_cluster(distinct_targets, duration_minutes, extra=7),
            })

    return pd.DataFrame(clusters)


def build_pattern_candidates(
    subscriptions: pd.DataFrame,
    memberships: pd.DataFrame,
    payments: pd.DataFrame,
    complaints: pd.DataFrame,
    min_users: int = 3,
    top_k: int = 10,
) -> pd.DataFrame:
    frames = [
        detect_owner_join_bursts(subscriptions, memberships, min_users=max(4, min_users), max_gap_minutes=30),
        detect_subscription_join_bursts(subscriptions, memberships, min_users=min_users, max_gap_minutes=20),
        detect_failed_payment_bursts(subscriptions, payments, min_users=min_users, max_gap_minutes=120),
        detect_complaint_bursts(subscriptions, complaints, min_targets=min_users, max_gap_minutes=1440),
    ]

    non_empty = [df for df in frames if not df.empty]
    if not non_empty:
        return pd.DataFrame(
            columns=[
                "pattern_id",
                "pattern_type",
                "label",
                "owner_id",
                "subscription_ids",
                "user_ids",
                "brands",
                "affected_users",
                "start_at",
                "end_at",
                "duration_minutes",
                "trigger_value",
                "evidence",
                "suspicious_score",
            ]
        )

    result = pd.concat(non_empty, ignore_index=True, sort=False)
    result = result.sort_values(
        ["suspicious_score", "affected_users"],
        ascending=[False, False],
    ).head(top_k)

    return result.reset_index(drop=True)