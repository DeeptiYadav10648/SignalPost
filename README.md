# SignalPost

Python 3.11+ company research runner for the current Builderr Signalpost batch contract. It reads evaluator-supplied organisation numbers, checks each against Brønnøysundregistrene (Brreg), and writes one terminal JSONL envelope per input in input order. It uses only the Python standard library; there is no LLM, API key, or paid service.

## Windows PowerShell setup

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m unittest discover -v
```

`requirements.txt` intentionally has no third-party packages.

## Official batch command

The starter command shape is preserved. The `--bulk` argument is the evaluator's frozen Brreg CSV identity snapshot; it is used when supplied, and every valid input still receives a live entity lookup so an old local cache cannot suppress research.

```powershell
python -m signalpost --organisations input\companies.jsonl --bulk input\brreg-enheter.csv --output output\envelopes.jsonl --profiles-output output\run-profiles.jsonl --report output\run-report.json --run-id local-001 --expected-count 100
```

Inputs may be JSON (`organisation_numbers` array), JSONL objects with `organisation_number` or `org_number`, or one number per text line. The result JSONL has one row for every input, including invalid identifiers and source failures. Each row carries an overall state (`available`, `not_available`, `blocked`, `not_applicable`, `ambiguous`, or `failed`), run timestamps, claims, evidence references, module states, changes, errors, and per-company request/runtime/cost metrics. The report records totals and evidence-reference validation. Output files are written atomically. `--max-requests` defaults to 1,800 across the whole batch; retries count against that shared cap. The batch runner uses sequential requests, an 8-second timeout, one retry, a 150 ms minimum request spacing, and a 40-minute deadline. It continues after an individual lookup error.

Local lookup/collection commands remain available:

```powershell
python -m signalpost lookup --org-number 810034882
python -m signalpost collect --target 1000 --max-requests 1800
python -m signalpost refresh --limit 100 --max-requests 200 --min-age-hours 24
python -m signalpost refresh --limit 100 --max-requests 200 --force
python -m signalpost validate
python -m signalpost export --format jsonl
python -m signalpost stats
```

`collect` stops without requests when the distinct stored count already meets `--target`. It resumes from the last fully processed API page, replaying an interrupted page safely through organization-number deduplication. `refresh` skips rows observed within the age threshold; `--force` explicitly rechecks the selected rows. The prior 24-hour refresh command correctly made no requests because the selected profiles had been collected less than 24 hours earlier. An API error or 404 does not erase stored facts.

## Sources and evidence

The primary source is the [official Brreg Open Data API documentation](https://data.brreg.no/enhetsregisteret/api/dokumentasjon/en/index.html), under the Norwegian Licence for Open Government Data (NLOD) 2.0. The runner uses the documented `GET /api/enheter/{orgnr}` endpoint; the local collector also uses the documented paginated `GET /api/enheter` search. The evaluator bulk snapshot, when supplied, is tied to the official `GET /api/enheter/lastned/csv` source and its SHA-256.

Live facts are published only after the returned organisation number matches the requested checksum-valid number. Each claim references an evidence record with the official record URL, UTC retrieval time, source update date when present, source-response SHA-256, and direct/derived marker. A 200 response without a matching identity is rejected as ambiguous. Missing fields stay missing; explicit `false` values remain false. The profile store compares nested values canonically, keeps before/after values for changes, rejects older observations, and preserves facts omitted from incomplete refreshes.

The currently implemented research lane is Brreg entity data. It does not yet fetch company websites, annual-account figures, roles, job adverts, or public activity. Those facts are not inferred or filled with placeholders. No external web crawling is performed.

## Reproducible checks and measured runs

Run offline tests with `python -m unittest discover -v`. The suite covers the official batch CLI shape, one result per input after individual errors, uncached live lookup via a mock, identity mismatches, request budgets/retries, evidence/timestamps, pagination resume, safe refreshes, history, and JSONL export.

`output/smoke-input.jsonl`, `output/smoke-envelopes.jsonl`, `output/smoke-profiles.jsonl`, and `output/smoke-report.json` are a live local smoke run: 100/100 envelopes, 100 distinct organizations, 100 available states, 100 requests, 155,296 ms, and USD 0 third-party cost. It used 99 organizations from the local test list plus uncached organisation number `812075462`. This is a local reliability check, not an official score. The official page currently specifies 50 recall/coverage, 30 precision/evidence, 12 synthesis, and 8 UX points; Builderr supplies fresh batches and does not score submitted precomputed profiles. See the [current challenge contract](https://builderr.ai/challenges/signalpost).

The existing `output/profiles.jsonl` and `output/signalpost.sqlite3` contain 1,000 distinct records with official Brreg entity URLs and UTC retrieval timestamps. The older cache predates response hashing: its raw response bodies were not retained and its per-fact response hashes are absent, so its original provenance is source-attributed but cannot be independently replay-verified from the stored files. The manifest records the actual current cache validation; the cache is for local testing, not an evaluator input or claimed score.

Expected API cost is USD 0 (public Brreg endpoints only); actual observed billable use is USD 0. The 100-company smoke used 100 requests. The earlier 1,000-profile collection used 10 paginated requests; a later `collect --target 1000` correctly used zero because the target was already met. No official evaluation, qualification, or submission is claimed.
