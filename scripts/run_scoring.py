import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.data_loader import load_tables
from app.services.preprocessing import preprocess_tables
from app.services.feature_engineering import build_user_features
from app.services.scoring import compute_score


DB_PATH = ROOT_DIR / "data" / "raw" / "risk_monitor_dataset.sqlite"
OUTPUT_PATH = ROOT_DIR / "data" / "processed" / "scored_subscribers.csv"


def main() -> None:
    users, subscriptions, memberships, payments, complaints = load_tables(DB_PATH)

    users, subscriptions, memberships, payments, complaints = preprocess_tables(
        users, subscriptions, memberships, payments, complaints
    )

    features = build_user_features(users, memberships, payments, complaints)
    scored = compute_score(features)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    scored.to_csv(OUTPUT_PATH, index=False)

    print(f"Scored file written to: {OUTPUT_PATH}")
    print(
        scored[
            [
                "user_id",
                "risk_score",
                "risk_level",
                "payments_count",
                "payment_failures_count",
                "complaints_count",
                "risky_exit_count",
                "top_risk_factors",
            ]
        ]
        .head(15)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()