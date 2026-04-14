import sys
import sqlite3
from pathlib import Path
from datetime import date

import pandas as pd
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.persistence import init_state_db, merge_actions, upsert_action


DB_PATH = ROOT_DIR / "data" / "raw" / "risk_monitor_dataset.sqlite"
SCORED_PATH = ROOT_DIR / "data" / "processed" / "scored_subscribers.csv"


st.set_page_config(
    page_title="Risk Monitor",
    page_icon="⚠️",
    layout="wide",
)


@st.cache_data
def load_scored_data() -> pd.DataFrame:
    if not SCORED_PATH.exists():
        raise FileNotFoundError(f"Scored file not found: {SCORED_PATH}")

    df = pd.read_csv(SCORED_PATH)

    datetime_cols = [
        "last_payment_at",
        "last_complaint_at",
        "last_joined_at",
        "signup_date_parsed",
        "last_seen_parsed",
    ]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    return df


@st.cache_data
def load_raw_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    conn = sqlite3.connect(DB_PATH)
    try:
        users = pd.read_sql_query("SELECT * FROM users", conn)
        memberships = pd.read_sql_query("SELECT * FROM memberships", conn)
        payments = pd.read_sql_query("SELECT * FROM payments", conn)
        complaints = pd.read_sql_query("SELECT * FROM complaints", conn)
    finally:
        conn.close()

    users["signup_date_parsed"] = pd.to_datetime(users["signup_date"], errors="coerce", utc=True)
    users["last_seen_parsed"] = pd.to_datetime(users["last_seen"], errors="coerce", utc=True)

    memberships["joined_at_parsed"] = pd.to_datetime(memberships["joined_at"], errors="coerce", utc=True)
    memberships["left_at_parsed"] = pd.to_datetime(memberships["left_at"], errors="coerce", utc=True)

    payments["created_at_parsed"] = pd.to_datetime(payments["created_at"], errors="coerce", utc=True)
    payments["captured_at_parsed"] = pd.to_datetime(payments["captured_at"], errors="coerce", utc=True)

    complaints["created_at_parsed"] = pd.to_datetime(complaints["created_at"], errors="coerce", utc=True)
    complaints["resolved_at_parsed"] = pd.to_datetime(complaints["resolved_at"], errors="coerce", utc=True)

    return users, memberships, payments, complaints


def build_dashboard_df() -> pd.DataFrame:
    scored = load_scored_data()
    dashboard_df = merge_actions(scored)
    return dashboard_df


def render_kpis(df: pd.DataFrame) -> None:
    total_users = len(df)
    high_risk = int((df["risk_level"] == "high").sum()) if "risk_level" in df.columns else 0
    critical_risk = int((df["risk_level"] == "critical").sum()) if "risk_level" in df.columns else 0
    watched = int((df["operator_action"] == "watch").sum()) if "operator_action" in df.columns else 0
    blocked = int((df["operator_action"] == "block").sum()) if "operator_action" in df.columns else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Subscribers", total_users)
    c2.metric("High risk", high_risk)
    c3.metric("Critical risk", critical_risk)
    c4.metric("Watched", watched)
    c5.metric("Blocked", blocked)


def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")

    min_score, max_score = st.sidebar.slider(
        "Risk score range",
        min_value=0.0,
        max_value=100.0,
        value=(0.0, float(min(100.0, df["risk_score"].max()))),
        step=1.0,
    )

    available_levels = sorted(df["risk_level"].dropna().unique().tolist())
    selected_levels = st.sidebar.multiselect(
        "Risk levels",
        options=available_levels,
        default=available_levels,
    )

    available_actions = sorted(df["operator_action"].dropna().unique().tolist())
    selected_actions = st.sidebar.multiselect(
        "Operator action",
        options=available_actions,
        default=available_actions,
    )

    status_col = "user_status" if "user_status" in df.columns else None
    selected_statuses = []
    if status_col is not None:
        available_statuses = sorted(
            df[status_col].dropna().unique().tolist(),
            key=lambda x: str(x),
        )
        selected_statuses = st.sidebar.multiselect(
            "User raw status code",
            options=available_statuses,
            default=available_statuses,
            help="Undocumented numeric status from the source dataset.",
        )

    country_col = "country_clean" if "country_clean" in df.columns else "country"
    countries = []
    if country_col in df.columns:
        countries = sorted(
            [c for c in df[country_col].dropna().astype(str).unique().tolist() if c != ""]
        )

    selected_countries = st.sidebar.multiselect(
        "Country",
        options=countries,
        default=[],
    )

    selected_date_range = None
    if "last_payment_at" in df.columns:
        valid_last_payment = pd.to_datetime(df["last_payment_at"], errors="coerce", utc=True).dropna()

        if not valid_last_payment.empty:
            min_date = valid_last_payment.min().date()
            max_date = valid_last_payment.max().date()

            selected_date_range = st.sidebar.date_input(
                "Last payment date range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )

    search_user = st.sidebar.text_input("Search by user_id")

    filtered = df.copy()
    filtered = filtered[
        (filtered["risk_score"] >= min_score) &
        (filtered["risk_score"] <= max_score)
    ]

    if selected_levels:
        filtered = filtered[filtered["risk_level"].isin(selected_levels)]

    if selected_actions:
        filtered = filtered[filtered["operator_action"].isin(selected_actions)]

    if status_col is not None and selected_statuses:
        filtered = filtered[filtered[status_col].isin(selected_statuses)]

    if selected_countries and country_col in filtered.columns:
        filtered = filtered[filtered[country_col].astype(str).isin(selected_countries)]

    if (
        selected_date_range is not None
        and isinstance(selected_date_range, (tuple, list))
        and len(selected_date_range) == 2
        and "last_payment_at" in filtered.columns
    ):
        start_date, end_date = selected_date_range

        last_payment_series = pd.to_datetime(filtered["last_payment_at"], errors="coerce", utc=True)

        filtered = filtered[
            last_payment_series.dt.date.between(start_date, end_date, inclusive="both")
            | last_payment_series.isna()
        ]

    if search_user.strip():
        filtered = filtered[
            filtered["user_id"].astype(str).str.contains(search_user.strip(), na=False)
        ]

    return filtered


def render_main_table(df: pd.DataFrame) -> None:
    st.subheader("Risk-ranked subscribers")

    display_cols = [
        "user_id",
        "risk_score",
        "risk_level",
        "user_status",
        "country_clean",
        "last_payment_at",
        "payments_count",
        "payment_failures_count",
        "complaints_count",
        "risky_exit_count",
        "operator_action",
        "top_risk_factors",
    ]

    existing_cols = [c for c in display_cols if c in df.columns]
    table_df = df[existing_cols].copy()

    if "risk_score" in table_df.columns:
        table_df["risk_score"] = table_df["risk_score"].round(2)

    if "last_payment_at" in table_df.columns:
        table_df["last_payment_at"] = (
            pd.to_datetime(table_df["last_payment_at"], errors="coerce", utc=True)
            .dt.strftime("%Y-%m-%d")
            .fillna("")
        )

    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
    )


def render_action_panel(selected_user_id: int, selected_row: pd.Series) -> None:
    st.subheader("Operator actions")

    default_note = selected_row.get("operator_note")
    if pd.isna(default_note):
        default_note = ""

    note = st.text_area(
        "Optional note",
        value=default_note,
        height=100,
        key=f"note_{selected_user_id}",
    )

    c1, c2, c3 = st.columns(3)

    if c1.button("Watch", use_container_width=True):
        upsert_action(selected_user_id, "watch", note=note if note.strip() else None)
        st.success(f"user_id {selected_user_id} marked as watch")
        st.rerun()

    if c2.button("Block", use_container_width=True):
        upsert_action(selected_user_id, "block", note=note if note.strip() else None)
        st.warning(f"user_id {selected_user_id} marked as block")
        st.rerun()

    if c3.button("Clear action", use_container_width=True):
        upsert_action(selected_user_id, "none", note=note if note.strip() else None)
        st.info(f"user_id {selected_user_id} action cleared")
        st.rerun()


def render_user_summary(selected_row: pd.Series) -> None:
    st.subheader("Subscriber summary")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Risk score", float(selected_row.get("risk_score", 0)))
    c2.metric("Risk level", str(selected_row.get("risk_level", "unknown")))
    c3.metric("Operator action", str(selected_row.get("operator_action", "none")))
    c4.metric("Complaints", int(selected_row.get("complaints_count", 0)))
    c5.metric("Failed payments", int(selected_row.get("payment_failures_count", 0)))

    meta_left, meta_right = st.columns(2)

    with meta_left:
        if "country_clean" in selected_row.index:
            st.write(f"**Country:** {selected_row.get('country_clean')}")
        if "user_status" in selected_row.index:
            st.write(f"**Raw status code:** {selected_row.get('user_status')}")
        if "last_payment_at" in selected_row.index and pd.notna(selected_row.get("last_payment_at")):
            last_payment = pd.to_datetime(selected_row.get("last_payment_at"), errors="coerce", utc=True)
            if pd.notna(last_payment):
                st.write(f"**Last payment:** {last_payment.strftime('%Y-%m-%d %H:%M')}")

    with meta_right:
        if "membership_count" in selected_row.index:
            st.write(f"**Memberships:** {int(selected_row.get('membership_count', 0))}")
        if "risky_exit_count" in selected_row.index:
            st.write(f"**Risky exits:** {int(selected_row.get('risky_exit_count', 0))}")
        if "payments_count" in selected_row.index:
            st.write(f"**Payments:** {int(selected_row.get('payments_count', 0))}")

    st.write("**Top risk factors**")
    st.info(selected_row.get("top_risk_factors", "No risk factor summary available."))

    if pd.notna(selected_row.get("operator_note", None)):
        st.write("**Operator note**")
        st.write(selected_row.get("operator_note"))

    if pd.notna(selected_row.get("action_updated_at", None)):
        st.caption(f"Last action update: {selected_row.get('action_updated_at')}")


def render_user_history(
    selected_user_id: int,
    users: pd.DataFrame,
    memberships: pd.DataFrame,
    payments: pd.DataFrame,
    complaints: pd.DataFrame,
) -> None:
    st.subheader(f"Detailed view — subscriber {selected_user_id}")

    user_row = users[users["id"] == selected_user_id].copy()

    user_memberships = memberships[memberships["user_id"] == selected_user_id].copy()
    user_memberships = user_memberships.sort_values("joined_at_parsed", ascending=False)

    user_payments = payments[payments["user_id"] == selected_user_id].copy()
    user_payments = user_payments.sort_values("created_at_parsed", ascending=False)

    user_complaints = complaints[complaints["target_id"] == selected_user_id].copy()
    user_complaints = user_complaints.sort_values("created_at_parsed", ascending=False)

    tab1, tab2, tab3, tab4 = st.tabs(["Profile", "Payments", "Memberships", "Complaints"])

    with tab1:
        st.dataframe(user_row, use_container_width=True, hide_index=True)

    with tab2:
        st.caption(f"{len(user_payments)} payment rows")
        st.dataframe(user_payments, use_container_width=True, hide_index=True)

    with tab3:
        st.caption(f"{len(user_memberships)} membership rows")
        st.dataframe(user_memberships, use_container_width=True, hide_index=True)

    with tab4:
        st.caption(f"{len(user_complaints)} complaint rows")
        st.dataframe(user_complaints, use_container_width=True, hide_index=True)


def main() -> None:
    init_state_db()

    st.title("Risk Monitor")
    st.caption("Internal tool for identifying and reviewing risky subscribers")

    try:
        dashboard_df = build_dashboard_df()
    except Exception as e:
        st.error(f"Failed to load scored data: {e}")
        st.stop()

    users, memberships, payments, complaints = load_raw_tables()

    render_kpis(dashboard_df)

    filtered_df = render_filters(dashboard_df)

    if filtered_df.empty:
        st.warning("No subscriber matches the current filters.")
        st.stop()

    filtered_df = filtered_df.sort_values(
        ["risk_score", "complaints_count", "payment_failures_count"],
        ascending=[False, False, False],
    )

    render_main_table(filtered_df)

    st.subheader("Open a subscriber")
    st.caption("Choose a subscriber from the filtered list to open the detailed view.")

    selected_user_id = st.selectbox(
        "Subscriber",
        options=filtered_df["user_id"].tolist(),
        format_func=lambda x: f"user_id {x}",
    )

    selected_row = filtered_df[filtered_df["user_id"] == selected_user_id].iloc[0]

    col_left, col_right = st.columns([2, 1])

    with col_left:
        render_user_summary(selected_row)
        render_user_history(selected_user_id, users, memberships, payments, complaints)

    with col_right:
        render_action_panel(int(selected_user_id), selected_row)


if __name__ == "__main__":
    main()