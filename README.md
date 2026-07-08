# tap-surveymonkey

`tap-surveymonkey` is a Singer tap for [SurveyMonkey](https://www.surveymonkey.com/),
an online survey platform for creating and distributing surveys and collecting and
analysing responses. This tap extracts surveys, their definitions, distribution
collectors, and individual responses via the SurveyMonkey API v3.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

Install from GitHub:

```bash
uv tool install git+https://github.com/MeltanoLabs/tap-surveymonkey.git@main
```

## Streams

| Stream | Endpoint | Replication | Replication Key |
|---|---|---|---|
| `surveys` | `/surveys` | Full table | — |
| `survey_details` | `/surveys/{id}/details` | Full table | — |
| `collectors` | `/surveys/{id}/collectors` | Full table | — |
| `responses` | `/surveys/{id}/responses/bulk` | Incremental | `date_modified` |

`survey_details`, `collectors` and `responses` are child streams of `surveys`
and are synced per survey.

## Configuration

### Accepted Config Options

| Setting | Required | Description |
|---|---|---|
| `access_token` | conditional | A SurveyMonkey OAuth access token, used directly as a bearer token. |
| `oauth_credentials` | conditional | Object of platform-managed OAuth credentials (`access_token`, `refresh_token`, `refresh_proxy_url`, `refresh_proxy_url_auth`). |
| `client_id` / `client_secret` | optional | SurveyMonkey OAuth app credentials, for direct token refresh. |
| `start_date` | optional | Earliest response date to sync, ISO-8601 (e.g. `2026-01-01T00:00:00Z`). Bookmark for `responses`. |
| `api_url` | optional | Regional API host for the account's data-residency region. Defaults to US `https://api.surveymonkey.com/v3`; use `https://api.eu.surveymonkey.com/v3` (EU) or `https://api.surveymonkey.ca/v3` (CA). |
| `user_agent` | optional | Optional `User-Agent` header. |

A full list of supported settings and capabilities is available by running:

```bash
tap-surveymonkey --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

SurveyMonkey uses OAuth 2.0. Requests are authenticated with a bearer access
token obtained by authorizing a SurveyMonkey OAuth app against the target
account. SurveyMonkey access tokens are typically long-lived, so this tap uses
the token directly; the `oauth_credentials` / `client_id` + `client_secret`
options are only needed when a token is issued with an expiry and must be
refreshed.

**Required app scopes** (grant the minimum needed for the streams you sync):

- `surveys_read` — read surveys and survey details
- `collectors_read` — read collectors
- `responses_read` — read responses

The authorizing SurveyMonkey user must have access to the surveys being
extracted. See the
[SurveyMonkey API docs](https://api.surveymonkey.com/v3/docs) for the OAuth flow.

### Data recovery / backfill

The `responses` stream replicates incrementally on `date_modified`, bookmarked
against `start_date`. To backfill or re-sync historical data, either lower
`start_date` to an earlier ISO-8601 timestamp, or reset the stream's state
(e.g. `meltano state clear` for the relevant state ID, or run with
`--state` omitted) so the sync starts again from `start_date`. `surveys`,
`survey_details` and `collectors` are full-table streams and always re-sync
completely.

## Usage

You can easily run `tap-surveymonkey` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-surveymonkey --version
tap-surveymonkey --help
tap-surveymonkey --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

You can also test the `tap-surveymonkey` CLI interface directly using `uv run`:

```bash
uv run tap-surveymonkey --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Use Meltano to run an EL pipeline:

```bash
# Install meltano
uv tool install meltano

# Test invocation
meltano invoke tap-surveymonkey --version

# Run a test EL pipeline
meltano run tap-surveymonkey target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
