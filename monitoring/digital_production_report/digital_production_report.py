#!/usr/bin/env python
"""
This posts some basic stats to the #wc-preservation channel at the
start of each month.  It reports the number of files ingested in
the digitised and born-digital spaces.

Example message:

    Stats for July 2023

    Born-digital: 6706

    Digitised:
    jp2,121850
    xml,82972
    mp4,52
    jpg,51
    mxf,51
    wav,32
    pdf,1

These are used by the Digital Production team for their reporting.

"""

import base64
import calendar
import datetime
import json
import urllib.request

import boto3


def read_secret(sess, *, secret_id):
    """
    Retrieve a secret from Secrets Manager.
    """
    client = sess.client("secretsmanager")
    return client.get_secret_value(SecretId=secret_id)["SecretString"]


def first_day_of_previous_month(date):
    """
    Returns the first day of the previous month.
    """
    last_day = last_day_of_previous_month(date)
    return datetime.date(last_day.year, last_day.month, 1)


def last_day_of_month(*, year, month):
    """
    Returns the last day of a given year/month.
    """
    # See https://stackoverflow.com/q/42950/1558022
    _, number_of_days = calendar.monthrange(year, month)

    return datetime.date(year, month, number_of_days)


def last_day_of_previous_month(date):
    """
    Returns the last day of the previous month.
    """
    if date.month == 1:
        prev_year, prev_month = date.year - 1, 12
    else:
        prev_year, prev_month = date.year, date.month - 1

    return last_day_of_month(year=prev_year, month=prev_month)


def previous_quarter_info(date):
    """
    Returns info about the previous quarter.
    """
    if 1 <= date.month <= 3:
        year = date.year - 1
        month_start, month_end = 10, 12
        label = "Q1"
    elif 4 <= date.month <= 6:
        year = date.year
        month_start, month_end = 1, 3
        label = "Q2"
    elif 7 <= date.month <= 9:
        year = date.year
        month_start, month_end = 4, 6
        label = "Q3"
    elif 10 <= date.month <= 12:
        year = date.year
        month_start, month_end = 7, 9
        label = "Q4"

    return (
        f"{year} {label}",
        datetime.date(year, month_start, 1),
        last_day_of_month(year=year, month=month_end),
    )


def born_digital_query(*, start_date, end_date):
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "createdDate": {
                                "gte": f"{start_date}T00:00:00.000Z",
                                "lte": f"{end_date}T23:59:59.999Z",
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                    {"prefix": {"name": "data/objects"}},
                ],
                "must_not": [
                    {"prefix": {"name": "data/objects/metadata"}},
                    {"prefix": {"name": "data/objects/submissionDocumentation"}},
                ],
                "filter": {
                    "terms": {"space": ["born-digital", "born-digital-accessions"]}
                },
            }
        }
    }


def digitised_query(*, start_date, end_date):
    return {
        "query": {
            "bool": {
                "must": {
                    "range": {
                        "createdDate": {
                            "gte": f"{start_date}T00:00:00.000Z",
                            "lte": f"{end_date}T23:59:59.999Z",
                            "format": "strict_date_optional_time",
                        }
                    }
                },
                "must_not": {"prefix": {"name": "data/b"}},
                "filter": {"term": {"space": "digitised"}},
            }
        },
        "aggs": {"suffixes": {"terms": {"field": "suffix", "size": 25}}},
        "size": 0,
    }


def run_query(*, es_host, es_user, es_pass, query_type, query):
    """
    Run a query against Elasticsearch and return the results.
    """
    es_url = f"https://{es_host}:9243/storage_files/_{query_type}"

    auth_string = f"{es_user}:{es_pass}".encode("ascii")
    auth_header = f"Basic " + base64.b64encode(auth_string).decode("ascii")

    print(f"Making query {json.dumps(query)}")
    req = urllib.request.Request(
        es_url,
        data=json.dumps(query).encode("utf8"),
        headers={"Content-Type": "application/json", "Authorization": auth_header},
        method="GET",
    )

    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())


def main(event, context):
    sess = boto3.Session()

    es_info = {
        "es_host": read_secret(sess, secret_id="reporting/es_host"),
        "es_pass": read_secret(sess, secret_id="reporting/read_only/es_password"),
        "es_user": read_secret(sess, secret_id="reporting/read_only/es_username"),
    }

    slack_webhook = read_secret(
        sess, secret_id="storage_service_reporter/slack_webhook"
    )

    today = datetime.date.today()

    prev_month_start = first_day_of_previous_month(today)
    prev_month_end = last_day_of_previous_month(today)

    born_digital_prev_month = run_query(
        **es_info,
        query_type="count",
        query=born_digital_query(start_date=prev_month_start, end_date=prev_month_end),
    )

    digitised_prev_month = run_query(
        **es_info,
        query_type="search",
        query=digitised_query(start_date=prev_month_start, end_date=prev_month_end),
    )

    lines = [f"*Stats for {prev_month_end.strftime('%B %Y')}*"]

    lines.append(f"Born-digital: {born_digital_prev_month['count']}")

    lines.append("Digitised:\n```")
    for entry in digitised_prev_month["aggregations"]["suffixes"]["buckets"]:
        lines.append(f"{entry['key']},{entry['doc_count']}")
    lines.append("```")

    # Just after the end of a quarter
    if today.month in (1, 4, 7, 10):
        prev_q_label, prev_q_start, prev_q_end = previous_quarter_info(today)

        born_digital_prev_quarter = run_query(
            **es_info,
            query_type="count",
            query=born_digital_query(start_date=prev_q_start, end_date=prev_q_end),
        )

        digitised_prev_quarter = run_query(
            **es_info,
            query_type="search",
            query=digitised_query(start_date=prev_q_start, end_date=prev_q_end),
        )

        lines.extend(["", f"*Stats for {prev_q_label}*"])

        lines.append(f"Born-digital: {born_digital_prev_quarter['count']}")

        lines.append("Digitised:\n```")
        for entry in digitised_prev_quarter["aggregations"]["suffixes"]["buckets"]:
            lines.append(f"{entry['key']},{entry['doc_count']}")
        lines.append("```")

    payload = {
        "username": "monthly-storage-service-stats",
        "icon_emoji": ":bar_chart:",
        "attachments": [{"fields": [{"value": "\n".join(lines)}]}],
    }

    req = urllib.request.Request(
        slack_webhook,
        data=json.dumps(payload).encode("utf8"),
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req)
    assert resp.status == 200, resp


if __name__ == "__main__":
    main(None, None)
