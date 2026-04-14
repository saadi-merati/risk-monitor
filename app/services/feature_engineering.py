import pandas as pd


def build_user_features(
    users: pd.DataFrame,
    memberships: pd.DataFrame,
    payments: pd.DataFrame,
    complaints: pd.DataFrame,
) -> pd.DataFrame:
    user_ids = pd.DataFrame({"user_id": sorted(users["id"].dropna().unique())})

    payment_features = (
        payments.groupby("user_id")
        .agg(
            payments_count=("id", "count"),
            payment_failures_count=("status_clean", lambda x: (x == "failed").sum()),
            payment_succeeded_count=("status_clean", lambda x: (x == "succeeded").sum()),
            payment_pending_count=("status_clean", lambda x: (x == "pending").sum()),
            payment_disputed_count=("status_clean", lambda x: (x == "disputed").sum()),
            payment_refunded_count=("status_clean", lambda x: (x == "refunded").sum()),
            last_payment_at=("created_at_parsed", "max"),
        )
        .reset_index()
    )

    payment_features["payment_failure_rate"] = (
        payment_features["payment_failures_count"] / payment_features["payments_count"]
    ).fillna(0)

    membership_features = (
        memberships.groupby("user_id")
        .agg(
            membership_count=("id", "count"),
            left_membership_count=("left_at_parsed", lambda x: x.notna().sum()),
            risky_exit_count=("reason_clean", lambda x: x.isin(
                ["fraud", "payment_failed", "owner_request", "inactive"]
            ).sum()),
            fraud_exit_count=("reason_clean", lambda x: (x == "fraud").sum()),
            payment_failed_exit_count=("reason_clean", lambda x: (x == "payment_failed").sum()),
            last_joined_at=("joined_at_parsed", "max"),
        )
        .reset_index()
    )

    complaint_features = (
        complaints.groupby("target_id")
        .agg(
            complaints_count=("id", "count"),
            open_complaints_count=("status_clean", lambda x: (x == "open").sum()),
            escalated_complaints_count=("status_clean", lambda x: (x == "escalated").sum()),
            in_progress_complaints_count=("status_clean", lambda x: (x == "in_progress").sum()),
            access_denied_complaints_count=("type_clean", lambda x: (x == "access_denied").sum()),
            fraud_suspicion_complaints_count=("type_clean", lambda x: (x == "fraud_suspicion").sum()),
            last_complaint_at=("created_at_parsed", "max"),
        )
        .reset_index()
        .rename(columns={"target_id": "user_id"})
    )

    user_meta = users[
        ["id", "country", "country_clean", "status", "signup_date_parsed", "last_seen_parsed"]
    ].rename(columns={"id": "user_id", "status": "user_status"})

    base = user_ids.merge(payment_features, on="user_id", how="left")
    base = base.merge(membership_features, on="user_id", how="left")
    base = base.merge(complaint_features, on="user_id", how="left")
    base = base.merge(user_meta, on="user_id", how="left")

    numeric_cols = [
        "payments_count",
        "payment_failures_count",
        "payment_succeeded_count",
        "payment_pending_count",
        "payment_disputed_count",
        "payment_refunded_count",
        "payment_failure_rate",
        "membership_count",
        "left_membership_count",
        "risky_exit_count",
        "fraud_exit_count",
        "payment_failed_exit_count",
        "complaints_count",
        "open_complaints_count",
        "escalated_complaints_count",
        "in_progress_complaints_count",
        "access_denied_complaints_count",
        "fraud_suspicion_complaints_count",
    ]

    for col in numeric_cols:
        if col in base.columns:
            base[col] = base[col].fillna(0)

    reference_now = pd.Timestamp.now("UTC")

    base["low_history_flag"] = (
        (base["payments_count"] <= 1) &
        (base["membership_count"] <= 1)
    ).astype(int)

    base["inactive_flag"] = (
        base["last_payment_at"].isna() |
        ((reference_now - base["last_payment_at"]).dt.days > 180)
    ).astype(int)

    return base