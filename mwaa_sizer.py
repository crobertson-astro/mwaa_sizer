#!/usr/bin/env python3
"""
MWAA Usage Estimator

Discovers all MWAA environments across AWS accounts in an organization,
collects environment configuration and CloudWatch usage metrics over the
last 30 days, and writes a CSV report to support right-sizing decisions.

Usage:
    python mwaa_sizer.py [--role ROLE] [--profile PROFILE] [--regions ...] \
                         [--accounts ...] [--hours N] [--output FILE] \
                         [--single-account] [-v]

CloudWatch retention note:
    30 days falls in the 15-63 day window, so period must be >= 300 s.
    Default is 3600 s (1 hour) to keep datapoint counts manageable.
"""

import csv
import datetime
import sys
import logging
import argparse

import boto3
from botocore.exceptions import ClientError

if sys.version_info < (3, 0):
    print("This script requires Python 3")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MWAA_REGIONS = [
    "us-east-1", "us-east-2", "us-west-2",
    "eu-west-1", "eu-west-2", "eu-central-1", "eu-north-1",
    "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2",
    "ap-south-1", "ca-central-1", "sa-east-1",
]

DEFAULT_HOURS = 720   # 30 days
DEFAULT_PERIOD = 3600  # 1-hour granularity (valid for 15-63 day window)

# Airflow-emitted metrics live under AmazonMWAA (tasks, heartbeat, parse time).
# Container-level CPU/Memory live under AWS/MWAA with a Cluster dimension.
AIRFLOW_NAMESPACE = "AmazonMWAA"
CONTAINER_NAMESPACE = "AWS/MWAA"

CSV_HEADERS = [
    "AccountId", "AccountName", "Region", "EnvironmentName",
    "Status", "AirflowVersion", "EnvironmentClass",
    "SchedulerCount", "MinWorkers", "MaxWorkers",
    # Airflow-level metrics (AmazonMWAA / Environment dimension)
    "WorkerCount_Avg", "WorkerCount_Max", "WorkerCount_P95",
    "RunningTasks_Avg", "RunningTasks_Max",
    "QueuedTasks_Avg", "QueuedTasks_Max",
    "SchedulerHeartbeat_Avg",
    "TotalParseTime_Avg_s", "TotalParseTime_Max_s",
    # Live instance counts from SampleCount of CPUUtilization (AWS/MWAA)
    "BaseWorkerCount_Avg", "BaseWorkerCount_Max",
    "AdditionalWorkerCount_Avg", "AdditionalWorkerCount_Max",
    "SchedulerActiveCount_Avg", "SchedulerActiveCount_Max",
    # BaseWorker CPU / Memory (AWS/MWAA, Cluster=BaseWorker)
    "BaseWorkerCPU_Avg_%", "BaseWorkerCPU_Max_%", "BaseWorkerCPU_P95_%",
    "BaseWorkerMemory_Avg_%", "BaseWorkerMemory_Max_%", "BaseWorkerMemory_P95_%",
    # AdditionalWorker CPU / Memory (AWS/MWAA, Cluster=AdditionalWorker)
    "AdditionalWorkerCPU_Avg_%", "AdditionalWorkerCPU_Max_%", "AdditionalWorkerCPU_P95_%",
    "AdditionalWorkerMemory_Avg_%", "AdditionalWorkerMemory_Max_%", "AdditionalWorkerMemory_P95_%",
    # Scheduler CPU / Memory (AWS/MWAA, Cluster=Scheduler)
    "SchedulerCPU_Avg_%", "SchedulerCPU_Max_%",
    "SchedulerMemory_Avg_%", "SchedulerMemory_Max_%",
    # WebServer CPU / Memory (AWS/MWAA, Cluster=WebServer)
    "WebServerCPU_Avg_%", "WebServerCPU_Max_%",
    "WebServerMemory_Avg_%", "WebServerMemory_Max_%",
]


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------

def get_org_accounts(session):
    org = session.client("organizations")
    accounts = []
    paginator = org.get_paginator("list_accounts")
    for page in paginator.paginate():
        for acct in page["Accounts"]:
            if acct["Status"] == "ACTIVE":
                accounts.append(acct)
    return accounts


def assume_role(session, account_id, role_name):
    sts = session.client("sts")
    role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    try:
        resp = sts.assume_role(RoleArn=role_arn, RoleSessionName="MWAASizer")
        creds = resp["Credentials"]
        return boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
    except ClientError as e:
        log.warning("Could not assume role %s: %s", role_arn, e)
        return None


def list_mwaa_environments(session, region):
    client = session.client("mwaa", region_name=region)
    try:
        envs = []
        paginator = client.get_paginator("list_environments")
        for page in paginator.paginate():
            envs.extend(page["Environments"])
        return envs
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in ("AccessDeniedException", "UnauthorizedException"):
            log.warning("Error listing MWAA in %s: %s", region, e)
        return []


def get_environment_details(session, region, env_name):
    client = session.client("mwaa", region_name=region)
    try:
        env = client.get_environment(Name=env_name)["Environment"]
        return {
            "name": env.get("Name"),
            "status": env.get("Status"),
            "airflow_version": env.get("AirflowVersion"),
            "environment_class": env.get("EnvironmentClass"),
            "schedulers": env.get("Schedulers"),
            "min_workers": env.get("MinWorkers"),
            "max_workers": env.get("MaxWorkers"),
        }
    except ClientError as e:
        log.warning("Could not get details for %s: %s", env_name, e)
        return None


# ---------------------------------------------------------------------------
# CloudWatch metric helpers
# ---------------------------------------------------------------------------

def _summarize(values):
    """Return avg/max/p95 for a list of numeric values, or None if empty."""
    if not values:
        return {"avg": None, "max": None, "p95": None}
    values = sorted(values)
    avg = sum(values) / len(values)
    p95 = values[min(int(len(values) * 0.95), len(values) - 1)]
    return {
        "avg": round(avg, 2),
        "max": round(max(values), 2),
        "p95": round(p95, 2),
    }


def get_metric(cw, namespace, env_name, metric_name, stat, hours, period, cluster=None):
    now = datetime.datetime.now(datetime.timezone.utc)
    dimensions = [{"Name": "Environment", "Value": env_name}]
    if cluster:
        dimensions.append({"Name": "Cluster", "Value": cluster})
    try:
        resp = cw.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=now - datetime.timedelta(hours=hours),
            EndTime=now,
            Period=period,
            Statistics=[stat],
        )
        return _summarize([dp[stat] for dp in resp["Datapoints"]])
    except ClientError as e:
        log.warning("Could not get metric %s for %s: %s", metric_name, env_name, e)
        return {"avg": None, "max": None, "p95": None}


def collect_metrics(session, region, env_name, hours, period):
    cw = session.client("cloudwatch", region_name=region)

    def airflow(metric, stat):
        return get_metric(cw, AIRFLOW_NAMESPACE, env_name, metric, stat, hours, period)

    def container(metric, stat, cluster):
        return get_metric(cw, CONTAINER_NAMESPACE, env_name, metric, stat, hours, period, cluster)

    return {
        # Airflow-level metrics
        "worker_count": airflow("WorkerCount", "Average"),
        "running":      airflow("RunningTasks", "Average"),
        "queued":       airflow("QueuedTasks", "Maximum"),
        "heartbeat":    airflow("SchedulerHeartbeat", "Average"),
        "parse_time":   airflow("TotalParseTime", "Average"),
        # Instance counts via SampleCount of CPUUtilization (AWS/MWAA)
        "base_worker_count":       container("CPUUtilization", "SampleCount", "BaseWorker"),
        "additional_worker_count": container("CPUUtilization", "SampleCount", "AdditionalWorker"),
        "scheduler_active_count":  container("CPUUtilization", "SampleCount", "Scheduler"),
        # BaseWorker CPU / Memory
        "base_worker_cpu": container("CPUUtilization",    "Average", "BaseWorker"),
        "base_worker_mem": container("MemoryUtilization", "Average", "BaseWorker"),
        # AdditionalWorker CPU / Memory
        "add_worker_cpu": container("CPUUtilization",    "Average", "AdditionalWorker"),
        "add_worker_mem": container("MemoryUtilization", "Average", "AdditionalWorker"),
        # Scheduler CPU / Memory
        "sched_cpu": container("CPUUtilization",    "Average", "Scheduler"),
        "sched_mem": container("MemoryUtilization", "Average", "Scheduler"),
        # WebServer CPU / Memory
        "web_cpu": container("CPUUtilization",    "Average", "WebServer"),
        "web_mem": container("MemoryUtilization", "Average", "WebServer"),
    }


# ---------------------------------------------------------------------------
# CSV row builder
# ---------------------------------------------------------------------------

def build_row(account_id, account_name, region, details, mx):
    wc   = mx["worker_count"]
    rn   = mx["running"]
    qu   = mx["queued"]
    hb   = mx["heartbeat"]
    pt   = mx["parse_time"]
    bwc  = mx["base_worker_count"]
    awc  = mx["additional_worker_count"]
    sac  = mx["scheduler_active_count"]
    bcpu = mx["base_worker_cpu"]
    bmem = mx["base_worker_mem"]
    acpu = mx["add_worker_cpu"]
    amem = mx["add_worker_mem"]
    scpu = mx["sched_cpu"]
    smem = mx["sched_mem"]
    xcpu = mx["web_cpu"]
    xmem = mx["web_mem"]

    return [
        account_id, account_name, region,
        details["name"], details["status"], details["airflow_version"],
        details["environment_class"], details["schedulers"],
        details["min_workers"], details["max_workers"],
        wc["avg"],   wc["max"],   wc["p95"],
        rn["avg"],   rn["max"],
        qu["avg"],   qu["max"],
        hb["avg"],
        pt["avg"],   pt["max"],
        bwc["avg"],  bwc["max"],
        awc["avg"],  awc["max"],
        sac["avg"],  sac["max"],
        bcpu["avg"], bcpu["max"], bcpu["p95"],
        bmem["avg"], bmem["max"], bmem["p95"],
        acpu["avg"], acpu["max"], acpu["p95"],
        amem["avg"], amem["max"], amem["p95"],
        scpu["avg"], scpu["max"],
        smem["avg"], smem["max"],
        xcpu["avg"], xcpu["max"],
        xmem["avg"], xmem["max"],
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="MWAA Usage Estimator — collect MWAA metrics across an AWS org"
    )
    parser.add_argument(
        "--role", default="OrganizationAccountAccessRole",
        help="IAM role to assume in each member account (default: OrganizationAccountAccessRole)",
    )
    parser.add_argument("--profile", help="AWS CLI profile for the management account")
    parser.add_argument(
        "--regions", nargs="+", default=MWAA_REGIONS,
        help="Regions to scan (default: all MWAA-supported regions)",
    )
    parser.add_argument(
        "--accounts", nargs="+",
        help="Specific account IDs to scan (default: all org accounts)",
    )
    parser.add_argument(
        "--hours", type=int, default=DEFAULT_HOURS,
        help=f"Hours of metric history (default: {DEFAULT_HOURS} = 30 days)",
    )
    parser.add_argument(
        "--period", type=int, default=DEFAULT_PERIOD,
        help=f"CloudWatch period in seconds (default: {DEFAULT_PERIOD}). "
             "Must be >= 300 for data older than 15 days.",
    )
    parser.add_argument(
        "--output", default=None,
        help="Output CSV file (default: mwaa_usage_YYYYMMDD_HHMMSS.csv)",
    )
    parser.add_argument(
        "--single-account", action="store_true",
        help="Scan the current account only; skip org lookup and role assumption",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output = args.output or f"mwaa_usage_{ts}.csv"

    session_kwargs = {"profile_name": args.profile} if args.profile else {}
    root_session = boto3.Session(**session_kwargs)

    if args.single_account:
        identity = root_session.client("sts").get_caller_identity()
        accounts_to_scan = [{"Id": identity["Account"], "Name": identity["Account"]}]
        use_role = False
    else:
        log.info("Fetching accounts from AWS Organizations...")
        try:
            org_accounts = get_org_accounts(root_session)
        except ClientError as e:
            log.error(
                "Could not list org accounts: %s. "
                "Use --single-account to scan just the current account.", e
            )
            sys.exit(1)

        if args.accounts:
            accounts_to_scan = [a for a in org_accounts if a["Id"] in args.accounts]
        else:
            accounts_to_scan = org_accounts

        use_role = True
        log.info("Found %d account(s) to scan", len(accounts_to_scan))

    rows_written = 0
    with open(output, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(CSV_HEADERS)

        for account in accounts_to_scan:
            account_id = account["Id"]
            account_name = account.get("Name", account_id)
            log.info("Scanning account %s (%s)...", account_id, account_name)

            if use_role:
                acct_session = assume_role(root_session, account_id, args.role)
                if acct_session is None:
                    log.warning("Skipping account %s — could not assume role", account_id)
                    continue
            else:
                acct_session = root_session

            for region in args.regions:
                log.debug("  Checking region %s...", region)
                env_names = list_mwaa_environments(acct_session, region)
                if not env_names:
                    continue

                log.info("  Found %d MWAA environment(s) in %s", len(env_names), region)
                for env_name in env_names:
                    log.info("    Collecting metrics for %s...", env_name)
                    details = get_environment_details(acct_session, region, env_name)
                    if details is None:
                        continue
                    metrics = collect_metrics(acct_session, region, env_name, args.hours, args.period)
                    writer.writerow(build_row(account_id, account_name, region, details, metrics))
                    rows_written += 1

    log.info("Done. %d environment(s) written to %s", rows_written, output)
    print(f"Report: {output} ({rows_written} environment(s))")


if __name__ == "__main__":
    main()
