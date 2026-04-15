# Risk Monitor

Internal web app for detecting, reviewing, and handling risky subscribers in a subscription-sharing marketplace.

This project was built for the Sharesub Chief of Staff / Operations case study. The goal was not to maximize the number of features, but to make explicit choices under time constraints, keep the system reproducible, and deliver a working tool with documented trade-offs.

---

## 1. Business context

The marketplace model is the following:

- an **owner** shares a subscription
- **subscribers** join and pay a monthly share
- the platform takes a **fee** on each payment
- when something goes wrong, an **issue / complaint** can be opened

The operational problem is that some subscribers create disproportionate friction and cost:

- repeated payment failures
- multiple complaints
- unstable subscription behavior
- suspicious coordinated patterns

The objective of this project is to help operations teams identify and review these profiles faster.

---

## 2. Project scope

The solution covers the 4 expected parts of the case study:

### A. Data cleaning and exploration

- exploratory notebook documenting anomalies and cleaning hypotheses
- inspection of schema, missing values, mixed timestamp formats, noisy categorical values, undocumented numeric status codes
- explicit cleaning previews before scoring

### B. Risk scoring

- deterministic risk score per subscriber
- no randomness
- no LLM dependency in the score
- one-command script that reads the SQLite database and writes a scored CSV

### C. Interface

- Streamlit internal tool
- ranked subscriber list
- filters by score, risk level, operator action, raw status code, country, last payment date, and user id
- subscriber workspace with overview, history, AI analyst, AI decider
- persistent operator actions (`watch`, `block`, `none`)

### D. AI agent

Three AI roles are implemented:

1. **Analyst**
   - structured profile summary
   - warning signals
   - comparison to baseline
   - decision support notes

2. **Decider**
   - recommended action: `ignore`, `monitor`, `warn`, `block`
   - confidence level
   - rationale
   - accept / reject feedback logging

3. **Pattern detector**
   - full-dataset suspicious cluster mining
   - AI review of top suspicious candidate clusters
   - examples: bursts of complaints, join bursts, payment failure bursts

---

## 3. Repository structure

```text
risk-monitor/
├── app/
│   ├── __init__.py
│   ├── main.py
│   └── services/
│       ├── __init__.py
│       ├── ai_agent.py
│       ├── ai_logging.py
│       ├── data_loader.py
│       ├── feature_engineering.py
│       ├── pattern_detector.py
│       ├── persistence.py
│       ├── preprocessing.py
│       └── scoring.py
├── data/
│   ├── raw/
│   │   └── risk_monitor_dataset.sqlite
│   └── processed/
│       └── scored_subscribers.csv
├── notebooks/
│   └── 01_eda_cleaning.ipynb
├── prompts/
│   ├── analyst_v1.md
│   ├── decision_v1.md
│   └── pattern_detector_v1.md
├── scripts/
│   ├── inspect_db.py
│   ├── profile_db.py
│   └── run_scoring.py
├── tests/
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
```

---

## 4. Quick start

### Git Bash / Unix-like shell

Run the whole project in 4 commands:

```bash
git clone https://github.com/saadi-merati/risk-monitor.git
cd risk-monitor
python -m venv .venv && source .venv/Scripts/activate && pip install -r requirements.txt
python scripts/run_scoring.py && streamlit run app/main.py
```

Then open the local Streamlit URL shown in the terminal, usually `http://localhost:8501`.

---

## 5. Environment variables

Create a `.env` file at the project root from `.env.example`.

Example:

```env
AI_API_KEY=
AI_BASE_URL=
AI_MODEL=
AI_INPUT_PRICE_PER_1M_TOKENS=
AI_OUTPUT_PRICE_PER_1M_TOKENS=
```

### Notes

- no API key is hardcoded
- the AI layer is designed for OpenAI-compatible APIs
- it can be configured for providers such as Groq
- if no API is configured, or if the API fails, the app falls back to deterministic local outputs for analyst / decider / pattern detector

---

## 6. Data source

The project uses a SQLite database containing 5 tables:

- `users`
- `subscriptions`
- `memberships`
- `payments`
- `complaints`

No official data dictionary was provided. The dataset is intentionally degraded and includes:

- inconsistent or missing values
- timestamps in mixed formats / timezones
- partially duplicated or contradictory rows
- undocumented numeric status codes

---

## 7. Data exploration and cleaning approach

The exploratory notebook is:

```text
notebooks/01_eda_cleaning.ipynb
```

### Main anomalies identified

#### 1. Mixed categorical values

Examples:

- `EUR`, `eur`, `€`, empty currency values
- `resolved`, `RESOLVED`, `Open`, `open`
- `ACCESS_DENIED`, `access_denied`, `Accès refusé`

#### 2. Mixed timestamp formats

Examples found in the dataset:

- ISO timestamps with timezone
- plain datetime strings
- slash-based datetime strings
- Unix-like numeric strings

#### 3. Missing values

Examples:

- `resolved_at` can be missing for unresolved complaints
- `left_at` can be missing for active memberships
- `last_payment_at` can be missing for users with no usable payment history

#### 4. Undocumented numeric status codes

Examples:

- `users.status`
- `subscriptions.status`
- `memberships.status`

These were not force-mapped to business labels without proof.

### Cleaning decisions

The project uses a conservative cleaning strategy:

- normalize noisy text categories for analysis
- parse dates with `errors="coerce"` to avoid crashing on invalid formats
- keep missing values when they carry business meaning
- aggregate to subscriber-level features with explicit handling of sparse history
- keep undocumented numeric status codes as raw codes in the interface rather than inventing labels

### Why I did not fully impute everything

Some nulls are informative:

- missing `resolved_at` can mean unresolved complaint
- missing `left_at` can mean active membership
- missing payment recency can mean no payment history

Because of that, I handled missingness explicitly in scoring and UI logic rather than blindly filling all nulls.

---

## 8. Risk scoring methodology

The scoring pipeline is deterministic and reproducible.

### Input

```text
data/raw/risk_monitor_dataset.sqlite
```

### Output

```text
data/processed/scored_subscribers.csv
```

### Command

```bash
python scripts/run_scoring.py
```

### High-level logic

The score is built at subscriber level from aggregated behavioral signals across:

- payments
- complaints
- membership history
- sparse / inactive history

### Main feature families

#### A. Payment features

Examples:

- `payments_count`
- `payment_failures_count`
- `payment_succeeded_count`
- `payment_disputed_count`
- `payment_failure_rate`
- `last_payment_at`

These capture direct operational friction and financial risk.

#### B. Complaint features

Examples:

- `complaints_count`
- `open_complaints_count`
- `escalated_complaints_count`
- `fraud_suspicion_complaints_count`
- `last_complaint_at`

These capture support burden and direct subscriber-related incidents.

#### C. Membership behavior features

Examples:

- `membership_count`
- `left_membership_count`
- `risky_exit_count`
- `fraud_exit_count`
- `payment_failed_exit_count`
- `last_joined_at`

These capture instability and problematic subscription exits.

#### D. Edge-case handling

Examples:

- `low_history_flag`
- `inactive_flag`

This avoids treating sparse history as equivalent to healthy history.

### Scoring philosophy

The final score is a weighted combination of:

- payment risk
- complaint risk
- membership instability
- uncertainty / inactivity signals

The scoring is:

- deterministic
- explainable
- stable
- independent from any LLM call

### Risk levels

The score is converted to:

- `low`
- `medium`
- `high`
- `critical`

### Why not train a machine learning model?

This dataset does not provide reliable labeled fraud / abuse outcomes. In that context, a rule-based score is more defensible because it is:

- reproducible
- explainable
- easy to challenge with operations teams
- robust under a short case-study time frame

---

## 9. Interface

The app is implemented with Streamlit.

### Main capabilities

#### Dashboard

- ranked subscriber table
- KPI summary
- filtering by:
  - risk score
  - risk level
  - operator action
  - raw status code
  - country
  - last payment date
  - user id

#### Subscriber workspace

The subscriber workspace is organized in tabs:

- Overview
- History
- AI Analyst
- AI Decider

This was introduced to reduce visual clutter and improve operator usability.

#### History view

Detailed history is split into:

- profile
- payments
- memberships
- complaints

#### Operator actions

Operators can persist:

- `watch`
- `block`
- clear action (`none`)

Persistence is stored locally in:

```text
data/app_state.sqlite
```

This ensures actions remain after refresh.

---

## 10. AI layer

The AI layer is implemented as a hybrid system:

- deterministic preparation and candidate extraction
- LLM summarization / recommendation on compact structured context

This keeps the system cheaper, more controllable, and easier to audit.

### 10.1 AI Analyst

When an operator opens a subscriber, the analyst generates:

- short summary
- observed behavior
- warning signals
- comparison to baseline
- decision support
- missing information

### 10.2 AI Decider

The decider generates:

- recommended action: `ignore`, `monitor`, `warn`, `block`
- confidence: `low`, `medium`, `high`
- rationale
- supporting evidence
- caution points
- missing information

The operator can:

- accept the recommendation
- reject the recommendation
- provide a rejection reason

Rejected decisions are logged for later analysis.

### 10.3 Pattern Detector

The pattern detector works in 2 stages:

#### Stage 1: deterministic cluster mining

Suspicious candidate clusters are mined across the dataset, including:

- owner join bursts
- subscription join bursts
- failed payment bursts
- complaint bursts

#### Stage 2: AI review

The top suspicious clusters are sent to the AI for:

- overall summary
- explanation of why each cluster is suspicious
- suggested follow-up
- limitations

### Why this design

I deliberately avoided sending the full raw dataset directly to the LLM and asking it to “find patterns”. Instead:

- clustering rules are deterministic and reproducible
- only the top suspicious candidates are sent to the model
- this reduces hallucination risk, token usage, and ambiguity

---

## 11. Prompt design

Prompts are versioned in:

```text
prompts/
```

Current prompt files:

- `analyst_v1.md`
- `decision_v1.md`
- `pattern_detector_v1.md`

### Prompt principles

Each prompt explicitly enforces:

- structured JSON output
- no unsupported invention
- conservative reasoning under weak evidence
- operational usefulness over generic summaries

This was important because the case study explicitly evaluates prompt quality, cost, limits, and traceability.

---

## 12. Cost, fallback, cache, traceability

### Cost control

The AI panels are on-demand:

- nothing is generated automatically on every page load
- operator clicks are required

This reduces unnecessary token consumption.

### Cache

Repeated identical AI requests reuse cached outputs when possible.

### Fallback

If:

- no API key is configured
- provider is unavailable
- prompt loading fails
- model output is invalid

the system falls back to deterministic local logic instead of crashing.

### Traceability

AI calls are logged with:

- input payload
- output payload
- role
- model
- prompt version
- success / failure
- estimated cost when configured

This information is stored in the local app state database.

---

## 13. Persistence layer

The persistence layer stores:

- operator actions (`watch`, `block`, `none`)
- AI decision feedback
- AI cache
- AI call logs

This is stored in:

```text
data/app_state.sqlite
```

This file is intentionally excluded from Git tracking.

---

## 14. What is currently implemented vs not implemented

### Implemented

- exploratory notebook
- deterministic risk scoring
- one-command scored CSV generation
- operator web app
- persistent actions
- AI analyst
- AI decider
- rejected decision logging
- hybrid pattern detector
- prompt versioning
- AI fallback / cache / logging

### Not implemented

- production deployment
- Docker packaging
- full automated test suite
- full reverse-engineering of all undocumented numeric status codes into business labels
- advanced global duplicate reconciliation across all tables

These omissions are deliberate trade-offs under time constraints, not hidden gaps.

---

## 15. Known limitations

### 1. Raw status codes remain raw

Undocumented numeric status codes are visible and filterable in the UI, but not fully translated into business states yet. I preferred preserving source truth over inventing a misleading mapping.

### 2. Duplicate / contradictory rows are documented, but not fully reconciled globally

The notebook identifies data quality issues and the scoring logic is made robust to them, but the project does not attempt a full record-linkage / deduplication layer.

### 3. Accepted `warn` recommendations

The AI decider can recommend `warn`, but the operator action persistence model is currently centered on `watch`, `block`, and `none`. As a result, `warn` is logged as decision feedback but is not persisted as a distinct long-term operator status.

### 4. Pattern detection uses predefined rules

The pattern detector is hybrid and useful, but still limited by the chosen candidate-generation rules. It is a suspicious signal detector, not a fraud proof engine.

### 5. Local execution focus

The application is currently designed to run locally rather than as a deployed production service.

---

## 16. Why these choices

The case study explicitly emphasizes:

- choice under constraints
- honest documentation
- working delivery over feature accumulation

Given the time box, I prioritized:

- reproducible scoring
- usable operator interface
- two strong AI roles
- traceability and failure handling
- a third AI-related feature through hybrid pattern detection

Instead of attempting a broader but weaker system.

---

## 17. Suggested next steps

If this project were extended, the most valuable next improvements would be:

- map raw status codes to defensible business states after deeper cross-checking
- deploy the app on a lightweight platform
- add automated tests for scoring, persistence, and AI fallback paths
- add stronger deduplication and contradiction resolution logic
- refine operator workflow around accepted `warn` actions
- add owner-centric investigation views for suspicious pattern clusters

---

## 18. Reproducibility checklist

### Generate scored output

```bash
python scripts/run_scoring.py
```

### Launch app

```bash
streamlit run app/main.py
```

### Notebook

```text
notebooks/01_eda_cleaning.ipynb
```

### Prompts

```text
prompts/
```
