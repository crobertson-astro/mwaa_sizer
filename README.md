# MWAA Sizer

Discovers all Amazon MWAA environments across an AWS organization, pulls 30 days of CloudWatch utilization metrics, and writes a CSV report to support right-sizing decisions.

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv)
- AWS credentials configured (CLI profile or environment variables)
- For org-wide scans: permissions to call `organizations:ListAccounts` and assume a role in each member account

## Setup

```bash
uv sync
```

## Usage

**Single account (current credentials):**
```bash
uv run python mwaa_sizer.py --single-account
```

**All accounts in an AWS org:**
```bash
uv run python mwaa_sizer.py --role OrganizationAccountAccessRole
```

**Specific accounts or regions:**
```bash
uv run python mwaa_sizer.py \
  --accounts 123456789012 987654321098 \
  --regions us-east-1 us-west-2
```

**Higher granularity (14 days at 1-minute resolution):**
```bash
uv run python mwaa_sizer.py --single-account --hours 336 --period 60
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--role` | `OrganizationAccountAccessRole` | IAM role to assume in each member account |
| `--profile` | *(default credentials)* | AWS CLI named profile for the management account |
| `--accounts` | *(all org accounts)* | Space-separated list of account IDs to scan |
| `--regions` | *(all MWAA regions)* | Space-separated list of regions to scan |
| `--hours` | `720` (30 days) | Length of metric history to collect |
| `--period` | `300` (5 min) | CloudWatch aggregation period in seconds |
| `--output` | `mwaa_usage_YYYYMMDD_HHMMSS.csv` | Output file path |
| `--single-account` | — | Skip org lookup; scan only the current account |
| `-v` | — | Verbose logging |

## Output

One row per MWAA environment. All CPU and memory values are averages across the requested window.

| Column | Description |
|--------|-------------|
| AccountId / AccountName / Region | Environment location |
| EnvironmentName / Status / AirflowVersion | Environment identity |
| EnvironmentClass | Instance class (`mw1.micro`, `mw1.small`, etc.) |
| SchedulerCount / MinWorkers / MaxWorkers | Configured capacity (from MWAA API) |
| BaseWorkerCount_Avg | Avg number of base workers running |
| AdditionalWorkerCount_Avg | Avg number of scale-out workers running |
| SchedulerActiveCount_Avg | Avg number of active schedulers |
| BaseWorkerCPU_Avg_% / BaseWorkerMemory_Avg_% | Base worker utilization |
| AdditionalWorkerCPU_Avg_% / AdditionalWorkerMemory_Avg_% | Additional worker utilization |
| SchedulerCPU_Avg_% / SchedulerMemory_Avg_% | Scheduler utilization |
| WebServerCPU_Avg_% / WebServerMemory_Avg_% | Web server utilization |

## CloudWatch retention limits

The default (30 days / 5-minute period) is the finest granularity CloudWatch retains for that window. Requesting a finer period over a longer window will result in empty data for the older portion.

| Window | Minimum period |
|--------|---------------|
| < 15 days | 60 s |
| 15–63 days | 300 s |
| > 63 days | 3600 s |

## Required IAM permissions

```json
{
  "Effect": "Allow",
  "Action": [
    "airflow:GetEnvironment",
    "airflow:ListEnvironments",
    "cloudwatch:GetMetricData",
    "organizations:ListAccounts",
    "sts:AssumeRole"
  ],
  "Resource": "*"
}
```
