# Personal KPI Dashboard

Automated weekly report system that measures **individual engineer growth metrics** across Jira, GitHub, and Rollbar.

Every Monday at 08:00 Cairo time a GitHub Actions workflow computes 8 rolling-30-day metrics per engineer, compares them to the previous 30 days, and delivers a **private Slack DM** to each engineer — no shared channel posts.

---

## 📊 Metrics Reference

| # | Metric | Formula | Data Source | Unit | Lower is Better |
|---|--------|---------|-------------|------|-----------------|
| 1 | **My Cycle Time** | Median days from first "In Progress" transition to first "Done" transition after it, for issues assigned to the engineer that were closed in the period | Jira changelog | days | ✅ |
| 2 | **My Resolved Contribution** | Sum of story points on issues marked Done in the period where assignee = engineer | Jira issue fields | story pts | ❌ |
| 3 | **My PR Merge Throughput** | Count of merged PRs authored by the engineer within the period | GitHub Search API | count | ❌ |
| 4 | **My Review Count** | Count of PR reviews submitted by the engineer (on PRs not authored by them) | GitHub reviews API | count | ❌ |
| 5 | **My Avg Time to First Review** | Average hours from PR `created_at` to the engineer's first review submission | GitHub reviews API | hours | ✅ |
| 6 | **My Code Review Speed** | Median hours from "Ready for Review" event (or `created_at` if not drafted) to the engineer's first comment or approval | GitHub timeline + reviews | hours | ✅ |
| 7 | **Errors Attributed to My Changes** | Count of new Rollbar production items (`first_seen` in period) where blame matches engineer identity. Unattributed items excluded. | Rollbar items API | count | ✅ |
| 8 | **My MTTR** | Median hours from Rollbar `first_seen` to `resolved_at` for items attributed to engineer. Falls back to Jira Done time if linked. Open items ignored. | Rollbar + optional Jira | hours | ✅ |
| 9 | **My CI Reliability** | % of workflow runs associated with the engineer's PRs that succeeded. Cancelled/skipped runs excluded from denominator. | GitHub Actions API | % | ❌ |

> **Trend calculation:** `% change = ((current − previous) / previous) × 100`. `N/A` when previous period has no data or value is 0.

---

## 🔧 Setup

### 1. Required GitHub Repository Secrets

Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `JIRA_BASE_URL` | Your Jira instance URL | `https://yourorg.atlassian.net` |
| `JIRA_EMAIL` | Atlassian account email for API auth | `bot@yourcompany.com` |
| `JIRA_API_TOKEN` | Atlassian API token ([create here](https://id.atlassian.com/manage-profile/security)) | `ATATT3...` |
| `JIRA_STORY_POINTS_FIELD` | Jira custom field ID for story points | `customfield_10016` |
| `GH_TOKEN` | GitHub PAT with `repo` + `read:org` scopes | `ghp_...` |
| `GH_REPO` | Repository in `owner/repo` format | `myorg/myrepo` |
| `GH_ORG` | GitHub organization name (optional) | `myorg` |
| `ROLLBAR_TOKEN` | Rollbar read-only project access token | `xxxxxxxx` |
| `ROLLBAR_PROJECT` | Rollbar project ID (numeric) | `12345` |
| `ROLLBAR_ENV` | Rollbar environment to query | `production` |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Full JSON content of a Google service account key file | `{"type":"service_account",...}` |
| `GOOGLE_SHEET_ID` | Spreadsheet ID from the Google Sheets URL | `1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms` |

### 2. Google Sheets Setup

#### 2a. Create a Google Cloud service account

1. Open [Google Cloud Console](https://console.cloud.google.com/) → **IAM & Admin → Service Accounts → Create Service Account**.
2. Give it a name (e.g. `kpi-pipeline`), click **Create and Continue**, then **Done**.
3. Click the service account → **Keys → Add Key → Create new key → JSON**.
4. Download the JSON file — this is the value you will paste into `GOOGLE_SERVICE_ACCOUNT_JSON`.

#### 2b. Enable the Sheets & Drive APIs

In the same Google Cloud project, go to **APIs & Services → Enable APIs** and enable:
- **Google Sheets API**
- **Google Drive API**

#### 2c. Create the spreadsheet and share it

1. Create a new Google Spreadsheet (or use an existing one).
2. Copy the **spreadsheet ID** from the URL — the long alphanumeric string between `/d/` and `/edit`. Paste it into `GOOGLE_SHEET_ID`.
3. Click **Share** and add the service account email (visible on the service account page, looks like `kpi-pipeline@your-project.iam.gserviceaccount.com`) with **Editor** access.

> **Privacy note:** Each engineer's metrics are written to their own worksheet tab named by their `google_sheet_tab` field. Engineers only see their own data if you share individual tabs — sharing at the spreadsheet level exposes all tabs.

### 3. Configure Engineers

Edit [`engineers.yml`](engineers.yml):

```yaml
engineers:
  - name: "Ahmed"
    jira_account_id: "5d9abc..."        # Jira account ID (stable, not username)
    github_login: "ahmed-dev"           # GitHub username
    rollbar_identity: "ahmed@co.com"    # Email as it appears in Rollbar blame
    google_sheet_tab: "Ahmed KPI"       # Worksheet tab name in the spreadsheet
```

**To add an engineer:** Add a new entry block and push to main.
**To remove an engineer:** Delete their entry block and push to main.
**No code changes needed** — the pipeline reads this file at runtime.

### 4. First Run

```bash
# Dry-run smoke test — Google Sheets credentials NOT required
# Prints the full Markdown report to stdout; writes CSV to /tmp; no Sheets writes.
cd d:/BILLQODE/Billqode\ KPIS
pip install -r kpis/requirements.txt
export JIRA_BASE_URL=https://yourorg.atlassian.net
export JIRA_EMAIL=you@co.com
export JIRA_API_TOKEN=...
export JIRA_STORY_POINTS_FIELD=customfield_10016
export GH_TOKEN=ghp_...
export GH_REPO=owner/repo
python kpis/src/main.py --dry_run
```

> `GOOGLE_SERVICE_ACCOUNT_JSON` and `GOOGLE_SHEET_ID` are **only required for live runs**.
> Omit them entirely when using `--dry_run` to test secrets, API connectivity, metric
> computation, and report generation without a Google Cloud project.

After confirming reports look correct, trigger the workflow manually:
**Actions → Personal KPI Dashboard → Run workflow**

---

## 📤 Output Channels

| Channel | Status | Notes |
|---------|--------|-------|
| **Google Sheets** | ✅ Implemented | Primary output. Each engineer has a dedicated worksheet tab. |
| Slack DM | 🔲 Not used | `slack_client.py` remains in repo but is not called by the pipeline. |
| Confluence | 🔲 TODO stub | Could create/update a personal page via Confluence REST API |

To add a new output channel: implement a new module in `kpis/src/output/` and call it from `main.py` after `render_markdown`.

---

## 🗂 Repository Structure

```
kpis/
├── README.md              ← This file
├── engineers.yml          ← Engineer roster (edit to add/remove)
├── requirements.txt       ← Python dependencies
└── src/
    ├── main.py            ← Orchestrator entry point
    ├── config.py          ← Env var + YAML loading
    ├── clients/           ← API wrappers
    │   ├── github_client.py
    │   ├── jira_client.py
    │   ├── rollbar_client.py
    │   └── slack_client.py
    ├── metrics/           ← One module per KPI
    │   ├── cycle_time.py
    │   ├── resolved_contribution.py
    │   ├── pr_merge_throughput.py
    │   ├── review_contribution.py
    │   ├── code_review_speed.py
    │   ├── errors_attributed.py
    │   ├── mttr.py
    │   └── ci_reliability.py
    ├── output/            ← Report renderers
    │   ├── render_markdown.py
    │   └── write_csv.py
    └── utils/             ← Shared helpers
        ├── dates.py
        ├── safe_run.py
        └── logging.py
```

---

## 🔬 Sample Output

![Sample Output](docs/sample_output.png)

> _Replace this placeholder with a screenshot of a real report once the pipeline has run._

**Example Markdown report structure:**

```
# 📊 Personal KPI Report — Ahmed

**Current period:** 2024-06-01 → 2024-07-01
**Previous period:** 2024-05-01 → 2024-06-01
*Generated: 2024-07-01 06:15 UTC*

| Metric                        | Current    | Previous   | Change   |
|-------------------------------|------------|------------|----------|
| My Cycle Time                 | 3.2 days   | 4.1 days   | ✅ -22.0% |
| My Resolved Contribution      | 34.0 pts   | 28.0 pts   | ✅ +21.4% |
| My PR Merge Throughput        | 12 count   | 9 count    | ✅ +33.3% |
| My Review Count               | 18 count   | 15 count   | ✅ +20.0% |
| My Avg Time to First Review   | 4.5 hours  | 6.2 hours  | ✅ -27.4% |
| My Code Review Speed          | 3.1 hours  | 5.0 hours  | ✅ -38.0% |
| Errors Attributed to My Changes | 2 count  | 4 count    | ✅ -50.0% |
| My MTTR                       | 6.5 hours  | 10.2 hours | ✅ -36.3% |
| My CI Reliability             | 94.1%      | 88.5%      | ✅ +6.3%  |
```

---

## 🐛 Troubleshooting

| Problem | Solution |
|---------|----------|
| `EnvironmentError: Required environment variable 'X' is not set` | Add the missing secret to GitHub repo secrets |
| `Jira metrics show N/A` | Check `JIRA_BASE_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN`. Run with `--dry_run` to see raw errors |
| `Rollbar metrics show N/A` | `ROLLBAR_TOKEN` and `ROLLBAR_PROJECT` are optional — if not set, those metrics are skipped. If set, check token has read access |
| Slack DM not received | Verify `SLACK_BOT_TOKEN` starts with `xoxb-` and the bot has `chat:write` scope |
| CI Reliability shows None | Engineer had no workflow runs for their PRs in this period — this is normal for quiet periods |

---

## 📐 Architecture Notes

- **Timezone handling:** All datetimes are UTC-aware. GitHub returns ISO-8601 with `Z`; Jira uses ISO-8601 offsets; Rollbar uses Unix epoch integers. All are normalised to `datetime` with `tzinfo=timezone.utc` before any arithmetic.
- **Rate limiting:** GitHub client checks `X-RateLimit-Remaining` after each call and sleeps 60s if < 50 remain. Jira uses 0.5s sleep between pages. Rollbar uses 0.3s sleep between pages.
- **Failure isolation:** Per-engineer `try/except` prevents one failure from blocking others. Per-source `try/except` means a Jira outage marks only Jira metrics as N/A while GitHub metrics still compute.
- **Privacy:** Reports are sent only to the individual engineer's Slack user ID. The admin receives only a pass/fail summary, not the metric values.
