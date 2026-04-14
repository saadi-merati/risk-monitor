import pandas as pd


def clean_text_series(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )


def parse_mixed_datetime(s: pd.Series) -> pd.Series:
    s_clean = s.copy()

    first_pass = pd.to_datetime(s_clean, errors="coerce", utc=True)

    mask_missing = first_pass.isna()
    if mask_missing.any():
        second_pass = pd.to_datetime(
            s_clean[mask_missing],
            errors="coerce",
            utc=True,
            dayfirst=True,
        )
        first_pass.loc[mask_missing] = second_pass

    mask_missing = first_pass.isna()
    if mask_missing.any():
        digits = s_clean[mask_missing].astype(str).str.strip()
        sec_mask = digits.str.fullmatch(r"\d{10}")
        ms_mask = digits.str.fullmatch(r"\d{13}")

        if sec_mask.any():
            first_pass.loc[digits[sec_mask].index] = pd.to_datetime(
                digits[sec_mask].astype("int64"),
                unit="s",
                errors="coerce",
                utc=True,
            )

        if ms_mask.any():
            first_pass.loc[digits[ms_mask].index] = pd.to_datetime(
                digits[ms_mask].astype("int64"),
                unit="ms",
                errors="coerce",
                utc=True,
            )

    return first_pass


def normalize_country(s: pd.Series) -> pd.Series:
    s = clean_text_series(s)

    mapping = {
        "fr": "FR",
        "fra": "FR",
        "france": "FR",
        "de": "DE",
        "es": "ES",
        "it": "IT",
        "be": "BE",
        "pt": "PT",
        "at": "AT",
        "ch": "CH",
        "nl": "NL",
        "": "",
    }

    normalized = s.replace(mapping)
    normalized = normalized.where(normalized.eq(""), normalized.str.upper())
    return normalized


def normalize_currency(s: pd.Series) -> pd.Series:
    s = clean_text_series(s)

    mapping = {
        "eur": "EUR",
        "€": "EUR",
        "usd": "USD",
        "gbp": "GBP",
        "": "",
    }
    return s.replace(mapping)


def normalize_payment_status(s: pd.Series) -> pd.Series:
    s = clean_text_series(s)

    mapping = {
        "succeeded": "succeeded",
        "success": "succeeded",
        "suceeded": "succeeded",
        "failed": "failed",
        "pending": "pending",
        "refunded": "refunded",
        "disputed": "disputed",
        "canceled": "canceled",
    }
    return s.replace(mapping)


def normalize_complaint_status(s: pd.Series) -> pd.Series:
    s = clean_text_series(s)

    mapping = {
        "open": "open",
        "resolved": "resolved",
        "closed": "closed",
        "in_progress": "in_progress",
        "escalated": "escalated",
    }
    return s.replace(mapping)


def normalize_complaint_type(s: pd.Series) -> pd.Series:
    s = clean_text_series(s)

    mapping = {
        "accès refusé": "access_denied",
        "access_denied": "access_denied",
        "access denied": "access_denied",
    }
    return s.replace(mapping)


def preprocess_tables(
    users: pd.DataFrame,
    subscriptions: pd.DataFrame,
    memberships: pd.DataFrame,
    payments: pd.DataFrame,
    complaints: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    users = users.copy()
    subscriptions = subscriptions.copy()
    memberships = memberships.copy()
    payments = payments.copy()
    complaints = complaints.copy()

    users["country_clean"] = normalize_country(users["country"])
    users["signup_date_parsed"] = parse_mixed_datetime(users["signup_date"])
    users["last_seen_parsed"] = parse_mixed_datetime(users["last_seen"])

    subscriptions["currency_clean"] = normalize_currency(subscriptions["currency"])
    subscriptions["created_at_parsed"] = parse_mixed_datetime(subscriptions["created_at"])

    memberships["joined_at_parsed"] = parse_mixed_datetime(memberships["joined_at"])
    memberships["left_at_parsed"] = parse_mixed_datetime(memberships["left_at"])
    memberships["reason_clean"] = clean_text_series(memberships["reason"])

    payments["status_clean"] = normalize_payment_status(payments["status"])
    payments["currency_clean"] = normalize_currency(payments["currency"])
    payments["created_at_parsed"] = parse_mixed_datetime(payments["created_at"])
    payments["captured_at_parsed"] = parse_mixed_datetime(payments["captured_at"])

    complaints["status_clean"] = normalize_complaint_status(complaints["status"])
    complaints["type_clean"] = normalize_complaint_type(complaints["type"])
    complaints["resolution_clean"] = clean_text_series(complaints["resolution"])
    complaints["created_at_parsed"] = parse_mixed_datetime(complaints["created_at"])
    complaints["resolved_at_parsed"] = parse_mixed_datetime(complaints["resolved_at"])

    return users, subscriptions, memberships, payments, complaints