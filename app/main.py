import sys
import sqlite3
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
import streamlit as st

from app.services.persistence import init_state_db, merge_actions, upsert_action


ROOT_DIR = Path(__file__).resolve().parents[1]
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
    if "user_id" not in df.columns:
        raise ValueError("Expected column 'user_id' in scored CSV")

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

    return users, memberships, payments, complaints


def build_dashboard_df() -> pd.DataFrame:
    scored = load_scored_data()
    dashboard_df = merge_actions(scored)
    return dashboard_df


def render_kpis(df: pd.DataFrame) -> None:
    total_users = len(df)
    high_risk = (df["risk_level"] == "high").sum() if "risk_level" in df.columns else 0
    critical_risk = (df["risk_level"] == "critical").sum() if "risk_level" in df.columns else 0
    watched = (df["operator_action"] == "watch").sum() if "operator_action" in df.columns else 0
    blocked = (df["operator_action"] == "block").sum() if "operator_action" in df.columns else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Subscribers", total_users)
    c2.metric("High risk", int(high_risk))
    c3.metric("Critical risk", int(critical_risk))
    c4.metric("Watched", int(watched))
    c5.metric("Blocked", int(blocked))


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

    country_col = "country_clean" if "country_clean" in df.columns else "country"
    if country_col in df.columns:
        countries = sorted([c for c in df[country_col].dropna().astype(str).unique().tolist() if c != ""])
    else:
        countries = []

    selected_countries = st.sidebar.multiselect(
        "Country",
        options=countries,
        default=[],
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

    if selected_countries and country_col in filtered.columns:
        filtered = filtered[filtered[country_col].astype(str).isin(selected_countries)]

    if search_user.strip():
        filtered = filtered[filtered["user_id"].astype(str).str.contains(search_user.strip(), na=False)]

    return filtered


def render_main_table(df: pd.DataFrame) -> None:
    st.subheader("Risk-ranked subscribers")

    display_cols = [
        "user_id",
        "risk_score",
        "risk_level",
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

    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
    )


def render_action_panel(selected_user_id: int) -> None:
    st.subheader("Operator actions")

    note = st.text_area("Optional note", value="", height=80, key=f"note_{selected_user_id}")

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

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Risk score", float(selected_row.get("risk_score", 0)))
    c2.metric("Risk level", str(selected_row.get("risk_level", "unknown")))
    c3.metric("Operator action", str(selected_row.get("operator_action", "none")))
    c4.metric("Complaints", int(selected_row.get("complaints_count", 0)))

    st.write("**Top risk factors**")
    st.write(selected_row.get("top_risk_factors", ""))

    if pd.notna(selected_row.get("operator_note", None)):
        st.write("**Operator note**")
        st.write(selected_row.get("operator_note"))

    if pd.notna(selected_row.get("action_updated_at", None)):
        st.caption(f"Last action update: {selected_row.get('action_updated_at')}")


def render_user_history(selected_user_id: int, users: pd.DataFrame, memberships: pd.DataFrame, payments: pd.DataFrame, complaints: pd.DataFrame) -> None:
    st.subheader("Detailed history")

    user_row = users[users["id"] == selected_user_id]
    user_memberships = memberships[memberships["user_id"] == selected_user_id].copy()
    user_payments = payments[payments["user_id"] == selected_user_id].copy()
    user_complaints = complaints[complaints["target_id"] == selected_user_id].copy()

    with st.expander("User profile", expanded=True):
        st.dataframe(user_row, use_container_width=True, hide_index=True)

    with st.expander("Membership history", expanded=True):
        st.dataframe(user_memberships, use_container_width=True, hide_index=True)

    with st.expander("Payment history", expanded=True):
        st.dataframe(user_payments, use_container_width=True, hide_index=True)

    with st.expander("Complaints history", expanded=True):
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

    st.subheader("Subscriber selection")
    selected_user_id = st.selectbox(
        "Choose a subscriber",
        options=filtered_df["user_id"].tolist(),
        format_func=lambda x: f"user_id {x}",
    )

    selected_row = filtered_df[filtered_df["user_id"] == selected_user_id].iloc[0]

    col_left, col_right = st.columns([2, 1])

    with col_left:
        render_user_summary(selected_row)
        render_user_history(selected_user_id, users, memberships, payments, complaints)

    with col_right:
        render_action_panel(int(selected_user_id))


if __name__ == "__main__":
    main()