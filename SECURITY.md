# Security and Privacy Notes

This project is a local analytical pipeline for an interview/take-home setting. It does not require secrets by default, but it does handle consultation-like operational data, so the main risks are data privacy, accidental commits, and uncontrolled raw payload retention.

## Data handling

- Do not commit `data/`, `warehouse.duckdb`, or other generated DuckDB files unless the dataset has been explicitly approved for sharing.
- `data/` and `*.duckdb` are ignored in `.gitignore`.
- `.env` is ignored so local path or configuration overrides are not committed.
- `raw.consultation_rejections.raw_payload` can store the rejected source row. This is useful for auditability, but in production it should be minimised, masked, or retained only for a defined period.

## External APIs

The pipeline calls Open-Meteo for:

- city geocoding;
- historical weather.

The pipeline sends city names and coordinates to Open-Meteo. It does not send consultation IDs, request timestamps, or request types to the API.

Production use should still document Open-Meteo as a third-party data processor/dependency and confirm that sharing city-level location data is acceptable.

## Secrets

Open-Meteo does not require an API key for this use case. If future APIs require credentials:

- load them from environment variables or a secret manager;
- never commit them to `.env.example`, docs, tests, notebooks, or logs;
- avoid storing secrets in DuckDB operational tables.

## Operational controls to add in production

- Retention policy for rejection payloads and API failure logs.
- Alerts for high rejection rates, low weather coverage, failed runs, and stale data.
- Reproducible dependency pinning with hashes or a lockfile.
- Manual review flow for ambiguous city matches.
- Access controls around the warehouse if real consultation data is used.
