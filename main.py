#!/usr/bin/env python3
"""
MWAA Usage Estimator

Discovers all MWAA environments across AWS accounts in an organization,
collects CloudWatch usage metrics (AWS/MWAA namespace) over the last 30 days,
and writes a CSV report to support right-sizing decisions.

Usage:
    python main.py [--role ROLE] [--profile PROFILE] [--regions ...] \
                         [--accounts ...] [--hours N] [--period N] \
                         [--output FILE] [--single-account] [-v]

CloudWatch retention note:
    30 days falls in the 15-63 day window, so period must be >= 300 s.
    Use --hours 336 --period 60 to get 14 days at 1-minute granularity instead.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import logging
import sys
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MWAA_REGIONS: list[str] = [
    "us-east-1", "us-east-2", "us-west-2",
    "eu-west-1", "eu-west-2", "eu-central-1", "eu-north-1",
    "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2",
    "ap-south-1", "ca-central-1", "sa-east-1",
]

CW_NAMESPACE = "AWS/MWAA"
DEFAULT_HOURS = 720   # 30 days — within the 63-day 5-minute retention window
DEFAULT_PERIOD = 300  # 5-minute buckets — finest granularity available for 30-day window


CSV_HEADERS = [
    "AccountId", "AccountName", "Region", "EnvironmentName",
    "Status", "AirflowVersion", "EnvironmentClass",
    "SchedulerCount", "MinWorkers", "MaxWorkers",
    "BaseWorkerCount_Avg",
    "AdditionalWorkerCount_Avg",
    "SchedulerActiveCount_Avg",
    "BaseWorkerCPU_Avg_%",       "BaseWorkerMemory_Avg_%",
    "AdditionalWorkerCPU_Avg_%", "AdditionalWorkerMemory_Avg_%",
    "SchedulerCPU_Avg_%",        "SchedulerMemory_Avg_%",
    "WebServerCPU_Avg_%",        "WebServerMemory_Avg_%",
]


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class EnvironmentConfig:
    name: str
    status: str | None
    airflow_version: str | None
    environment_class: str | None
    scheduler_count: int | None
    min_workers: int | None
    max_workers: int | None


@dataclass
class ClusterMetrics:
    cpu_avg: float | None
    memory_avg: float | None


@dataclass
class EnvironmentMetrics:
    base_worker_count_avg: float | None
    additional_worker_count_avg: float | None
    scheduler_active_count_avg: float | None
    base_worker: ClusterMetrics
    additional_worker: ClusterMetrics
    scheduler: ClusterMetrics
    webserver: ClusterMetrics


@dataclass
class EnvironmentReport:
    account_id: str
    account_name: str
    region: str
    config: EnvironmentConfig
    metrics: EnvironmentMetrics

    def to_csv_row(self) -> list:
        c, m = self.config, self.metrics
        return [
            self.account_id,              self.account_name,          self.region,
            c.name,                       c.status,                   c.airflow_version,
            c.environment_class,          c.scheduler_count,
            c.min_workers,                c.max_workers,
            m.base_worker_count_avg,
            m.additional_worker_count_avg,
            m.scheduler_active_count_avg,
            m.base_worker.cpu_avg,        m.base_worker.memory_avg,
            m.additional_worker.cpu_avg,  m.additional_worker.memory_avg,
            m.scheduler.cpu_avg,          m.scheduler.memory_avg,
            m.webserver.cpu_avg,          m.webserver.memory_avg,
        ]


# ---------------------------------------------------------------------------
# AWS / org helpers
# ---------------------------------------------------------------------------

def get_org_accounts(session: boto3.Session) -> list[dict]:
    org = session.client("organizations")
    accounts: list[dict] = []
    for page in org.get_paginator("list_accounts").paginate():
        accounts.extend(a for a in page["Accounts"] if a["Status"] == "ACTIVE")
    return accounts


def assume_role(
    session: boto3.Session, account_id: str, role_name: str
) -> boto3.Session | None:
    sts = session.client("sts")
    role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    try:
        creds = sts.assume_role(RoleArn=role_arn, RoleSessionName="MWAASizer")["Credentials"]
        return boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
    except ClientError as e:
        log.warning("Could not assume role %s: %s", role_arn, e)
        return None


# ---------------------------------------------------------------------------
# MWAA discovery
# ---------------------------------------------------------------------------

def list_mwaa_environments(session: boto3.Session, region: str) -> list[str]:
    client = session.client("mwaa", region_name=region)
    try:
        names: list[str] = []
        for page in client.get_paginator("list_environments").paginate():
            names.extend(page["Environments"])
        return names
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("AccessDeniedException", "UnauthorizedException"):
            log.warning("Error listing MWAA in %s: %s", region, e)
        return []


def get_environment_config(
    session: boto3.Session, region: str, env_name: str
) -> EnvironmentConfig | None:
    try:
        env = session.client("mwaa", region_name=region).get_environment(Name=env_name)["Environment"]
        return EnvironmentConfig(
            name=env["Name"],
            status=env.get("Status"),
            airflow_version=env.get("AirflowVersion"),
            environment_class=env.get("EnvironmentClass"),
            scheduler_count=env.get("Schedulers"),
            min_workers=env.get("MinWorkers"),
            max_workers=env.get("MaxWorkers"),
        )
    except ClientError as e:
        log.warning("Could not get config for %s: %s", env_name, e)
        return None


# ---------------------------------------------------------------------------
# CloudWatch metric collection
# ---------------------------------------------------------------------------

def _avg_metric(
    cw,
    env_name: str,
    metric_name: str,
    stat: str,
    cluster: str,
    hours: int,
    period: int,
) -> float | None:
    now = datetime.datetime.now(datetime.timezone.utc)
    kwargs: dict = {
        "MetricDataQueries": [{
            "Id": "m1",
            "MetricStat": {
                "Metric": {
                    "Namespace": CW_NAMESPACE,
                    "MetricName": metric_name,
                    "Dimensions": [
                        {"Name": "Environment", "Value": env_name},
                        {"Name": "Cluster",     "Value": cluster},
                    ],
                },
                "Period": period,
                "Stat": stat,
            },
        }],
        "StartTime": now - datetime.timedelta(hours=hours),
        "EndTime": now,
    }
    values: list[float] = []
    try:
        while True:
            resp = cw.get_metric_data(**kwargs)
            values.extend(resp["MetricDataResults"][0]["Values"])
            if next_token := resp.get("NextToken"):
                kwargs["NextToken"] = next_token
            else:
                break
    except ClientError as e:
        log.warning("Could not get %s/%s for %s: %s", cluster, metric_name, env_name, e)
        return None
    return round(sum(values) / len(values), 2) if values else None


def collect_environment_metrics(
    session: boto3.Session, region: str, env_name: str, hours: int, period: int
) -> EnvironmentMetrics:
    cw = session.client("cloudwatch", region_name=region)

    def cpu(cluster: str) -> float | None:
        return _avg_metric(cw, env_name, "CPUUtilization", "Average", cluster, hours, period)

    def mem(cluster: str) -> float | None:
        return _avg_metric(cw, env_name, "MemoryUtilization", "Average", cluster, hours, period)

    def count(cluster: str) -> float | None:
        # SampleCount = containers × (period / 60) because each container emits once per minute.
        raw = _avg_metric(cw, env_name, "CPUUtilization", "SampleCount", cluster, hours, period)
        return round(raw / (period / 60), 2) if raw is not None else None

    return EnvironmentMetrics(
        base_worker_count_avg=count("BaseWorker"),
        additional_worker_count_avg=count("AdditionalWorker"),
        scheduler_active_count_avg=count("Scheduler"),
        base_worker=ClusterMetrics(cpu("BaseWorker"),       mem("BaseWorker")),
        additional_worker=ClusterMetrics(cpu("AdditionalWorker"), mem("AdditionalWorker")),
        scheduler=ClusterMetrics(cpu("Scheduler"),          mem("Scheduler")),
        webserver=ClusterMetrics(cpu("WebServer"),          mem("WebServer")),
    )


# ---------------------------------------------------------------------------
# Report writing
# ---------------------------------------------------------------------------

def write_report(reports: list[EnvironmentReport], output_path: str) -> None:
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(CSV_HEADERS)
        for report in reports:
            writer.writerow(report.to_csv_row())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MWAA Usage Estimator — collect metrics across an AWS org"
    )
    parser.add_argument(
        "--role", default="OrganizationAccountAccessRole",
        help="IAM role to assume in each member account",
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
    return parser.parse_args()


def resolve_accounts(
    root_session: boto3.Session,
    single_account: bool,
    account_filter: list[str] | None,
) -> tuple[list[dict], bool]:
    """Return (accounts_to_scan, use_role)."""
    if single_account:
        account_id = root_session.client("sts").get_caller_identity()["Account"]
        return [{"Id": account_id, "Name": account_id}], False

    log.info("Fetching accounts from AWS Organizations...")
    try:
        all_accounts = get_org_accounts(root_session)
    except ClientError as e:
        log.error("Could not list org accounts: %s. Use --single-account to scan one account.", e)
        sys.exit(1)

    accounts = (
        [a for a in all_accounts if a["Id"] in account_filter]
        if account_filter
        else all_accounts
    )
    log.info("Found %d account(s) to scan", len(accounts))
    return accounts, True


def main() -> None:
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output = args.output or f"mwaa_usage_{ts}.csv"

    root_session = boto3.Session(**{"profile_name": args.profile} if args.profile else {})
    accounts, use_role = resolve_accounts(root_session, args.single_account, args.accounts)

    reports: list[EnvironmentReport] = []

    for account in accounts:
        account_id: str = account["Id"]
        account_name: str = account.get("Name", account_id)
        log.info("Scanning account %s (%s)...", account_id, account_name)

        if use_role:
            session = assume_role(root_session, account_id, args.role)
            if session is None:
                log.warning("Skipping account %s — could not assume role", account_id)
                continue
        else:
            session = root_session

        for region in args.regions:
            log.debug("  Checking region %s...", region)
            env_names = list_mwaa_environments(session, region)
            if not env_names:
                continue

            log.info("  Found %d MWAA environment(s) in %s", len(env_names), region)
            for env_name in env_names:
                log.info("    Collecting metrics for %s...", env_name)
                config = get_environment_config(session, region, env_name)
                if config is None:
                    continue
                metrics = collect_environment_metrics(
                    session, region, env_name, args.hours, args.period
                )
                reports.append(EnvironmentReport(account_id, account_name, region, config, metrics))

    write_report(reports, output)
    log.info("Done. %d environment(s) written to %s", len(reports), output)
    print(f"Report: {output} ({len(reports)} environment(s))")


if __name__ == "__main__":
    main()
