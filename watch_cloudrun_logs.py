#!/usr/bin/env python3
"""
Watch Google Cloud Run Job logs in real-time

Usage:
    python watch_cloudrun_logs.py
    python watch_cloudrun_logs.py --execution-name exp-scraper-job-abc123
"""

import subprocess
import time
import sys
import json
from datetime import datetime, timezone
import argparse

# Configuration
PROJECT_ID = "massuh-420617"
REGION = "southamerica-east1"
JOB_NAME = "exp-scraper-job"
POLL_INTERVAL = 3  # seconds


def get_latest_execution():
    """Get the most recent execution ID"""
    try:
        cmd = [
            "gcloud", "run", "jobs", "executions", "list",
            f"--job={JOB_NAME}",
            f"--region={REGION}",
            "--limit=1",
            "--format=value(name)"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        execution_name = result.stdout.strip()
        if execution_name:
            return execution_name
        else:
            print("❌ No executions found")
            return None
    except subprocess.CalledProcessError as e:
        print(f"❌ Error getting execution: {e.stderr}")
        return None


def fetch_logs(execution_name=None, last_timestamp=None):
    """Fetch logs from Cloud Logging"""

    # Build filter
    if execution_name:
        filter_str = (
            f'resource.type="cloud_run_job" '
            f'AND resource.labels.job_name="{JOB_NAME}" '
            f'AND resource.labels.execution_name="{execution_name}"'
        )
    else:
        filter_str = (
            f'resource.type="cloud_run_job" '
            f'AND resource.labels.job_name="{JOB_NAME}"'
        )

    # Add timestamp filter to get only new logs
    if last_timestamp:
        filter_str += f' AND timestamp>"{last_timestamp}"'

    cmd = [
        "gcloud", "logging", "read",
        filter_str,
        "--limit=100",
        "--format=json",
        f"--project={PROJECT_ID}"
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if result.stdout.strip():
            logs = json.loads(result.stdout)
            return logs
        return []
    except subprocess.CalledProcessError as e:
        print(f"⚠️  Error fetching logs: {e.stderr}")
        return []
    except json.JSONDecodeError:
        return []


def format_log_entry(log):
    """Format a log entry for display"""
    timestamp = log.get('timestamp', '')

    # Try to get log text
    text = (
        log.get('textPayload') or
        log.get('jsonPayload', {}).get('message') or
        str(log.get('jsonPayload', ''))
    )

    # Format timestamp
    try:
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        time_str = dt.strftime('%H:%M:%S')
    except:
        time_str = timestamp[:8] if timestamp else '??:??:??'

    # Get severity
    severity = log.get('severity', 'INFO')

    # Color based on severity
    if severity == 'ERROR':
        prefix = '❌'
    elif severity == 'WARNING':
        prefix = '⚠️ '
    elif severity == 'INFO':
        prefix = 'ℹ️ '
    else:
        prefix = '  '

    return f"{time_str} {prefix} {text}"


def watch_logs(execution_name=None):
    """Continuously watch and display logs"""

    if not execution_name:
        print(f"🔍 Looking for latest execution of {JOB_NAME}...")
        execution_name = get_latest_execution()
        if not execution_name:
            print("No running executions found. Start a job first:")
            print(f"  gcloud run jobs execute {JOB_NAME} --region={REGION}")
            return

    print("=" * 80)
    print(f"📋 Watching logs for: {execution_name}")
    print(f"🔄 Refreshing every {POLL_INTERVAL}s (Ctrl+C to stop)")
    print("=" * 80)
    print()

    last_timestamp = None
    seen_logs = set()

    try:
        while True:
            logs = fetch_logs(execution_name, last_timestamp)

            if logs:
                # Sort by timestamp (oldest first)
                logs.sort(key=lambda x: x.get('timestamp', ''))

                for log in logs:
                    # Use insertId to deduplicate
                    log_id = log.get('insertId')
                    if log_id and log_id not in seen_logs:
                        seen_logs.add(log_id)
                        print(format_log_entry(log))

                        # Update last timestamp
                        timestamp = log.get('timestamp')
                        if timestamp:
                            last_timestamp = timestamp

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n👋 Stopped watching logs")
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description='Watch Google Cloud Run Job logs in real-time',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python watch_cloudrun_logs.py
  python watch_cloudrun_logs.py --execution-name exp-scraper-job-abc123
        """
    )

    parser.add_argument(
        '--execution-name',
        help='Specific execution name to watch (default: latest)'
    )

    args = parser.parse_args()

    watch_logs(args.execution_name)


if __name__ == '__main__':
    main()
