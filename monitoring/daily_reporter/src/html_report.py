import datetime
import os
import re

import boto3
from jinja2 import Environment, FileSystemLoader, select_autoescape


def last_event(ingest):
    failures = [ev for ev in ingest['events'] if 'failed' in ev['description']]
    if failures:
        return max(failures, key=lambda ev: ev['createdDate'])
    else:
        try:
            return max(ingest['events'], key=lambda ev: ev['createdDate'])
        except ValueError:
            return None


def add_s3_uri(description):
    s3_uri = re.search(r's3://(?P<bucket>[^/]+)/(?P<key>[^\s:]+)', description)

    if s3_uri is None:
        return description
    else:
        bucket = s3_uri.group("bucket")
        key = s3_uri.group("key")

        s3_console_link = (
            f"https://s3.console.aws.amazon.com/s3/buckets/{bucket}"
            f"/{os.path.dirname(key)}/?tab=overview&prefixSearch={os.path.basename(key)}"
            f"&region=eu-west-1"
        )

        return description.replace(s3_uri.group(0), f'<a href="{s3_console_link}">{s3_uri.group(0)}</a>')


def create_html_report(classified_ingests, found_everything, days_to_fetch):
    template_dir = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), 'templates'
    )
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(['html'])
    )

    env.filters["last_event"] = last_event
    env.filters["add_s3_uri"] = add_s3_uri

    template = env.get_template('report.html')

    return template.render(
        classified_ingests=classified_ingests,
        found_everything=found_everything,
        days_to_fetch=days_to_fetch,
        today=datetime.date.today(),
        now=datetime.datetime.now(),
    )


def store_s3_report(**kwargs):
    html = create_html_report(**kwargs)

    s3 = boto3.client("s3")

    today = datetime.date.today().strftime("%Y-%m-%d")

    daily_reporting_bucket = "wellcomecollection-storage-daily-reporting"

    s3.put_object(
        Bucket=daily_reporting_bucket,
        Key=f"{today}.html",
        Body=html.encode("utf8"),
        ContentType="text/html",
        ACL="public-read"
    )

    return f"https://{daily_reporting_bucket}.s3-eu-west-1.amazonaws.com/{today}.html"
