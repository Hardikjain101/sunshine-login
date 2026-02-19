# HR Attendance Analytics - Production Architecture

This refactor introduces a production-ready folder structure for AWS deployment while preserving the validated dashboard behavior.

## Current Non-Breaking Strategy

- `Final.py` remains the validated logic source.
- New modules provide production architecture boundaries (`config`, `database`, `services`, `components`, `utils`, `models`).
- `app.py` is now the official minimal entry point and boots infrastructure before running the dashboard.

This allows safe, incremental extraction without altering business outcomes.

## Folder Structure

```text
project-root/
├── app.py
├── config/
│   ├── __init__.py
│   ├── settings.py
│   └── database.py
├── database/
│   ├── __init__.py
│   ├── connection.py
│   ├── queries.py
│   └── repository.py
├── services/
│   ├── __init__.py
│   ├── attendance_service.py
│   ├── meal_service.py
│   ├── overtime_service.py
│   ├── compliance_service.py
│   ├── annotation_service.py
│   └── holiday_service.py
├── models/
│   ├── __init__.py
│   └── schemas.py
├── utils/
│   ├── __init__.py
│   ├── helpers.py
│   ├── date_utils.py
│   └── formatters.py
├── components/
│   ├── __init__.py
│   ├── calendar_component.py
│   ├── kpi_component.py
│   ├── charts_component.py
│   └── filters_component.py
├── static/
│   └── styles.css
├── Final.py
├── requirements.txt
├── .env
└── README.md
```

## Run Locally

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Update `.env` values for your DB.
3. Run:
   - `streamlit run app.py`

## AWS Deployment Notes

- Use environment variables in ECS/EC2/Beanstalk/Containers (do not store secrets in code).
- Keep Streamlit instances stateless; MySQL holds persistent annotation data.
- DB pooling is centralized via `database/connection.py`.
- SQL is centralized in `database/queries.py`.
- Repository pattern is implemented in `database/repository.py`.

## DB Index Suggestion

Run this in production migration tooling if annotation volume grows:

```sql
CREATE INDEX idx_annotations_type_date
ON attendance_annotations(annotation_type, `date`);
```

## Important

This refactor is intentionally compatibility-first:

- Business logic, KPI math, overtime rules, meal rules, and attendance behavior remain unchanged.
- New modules are in place for progressive extraction from `Final.py` with controlled risk.
