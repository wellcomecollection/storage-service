import fire
import aws
import time
import uuid
import datetime
import dateutil
import settings
import requests
import json
import storage_api
import dds
import status_table
import migration_report
from mets_filesource import bnumber_generator


def json_default(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()


class MigrationTool(object):
    def update_status(self, delay, filter=""):
        update_bag_and_ingest_status(delay, filter)

    def update_dds_status(self, delay, filter="", force=False):
        update_dds_status_slow(delay, filter, force)

    def ingest(self, delay, filter=""):
        do_ingest(delay, filter)

    def simulate_goobi_calls(self, delay, filter=None, total=1, after="2019-03-15"):
        call_dds(delay, filter, total, after)

    def ensure_population(self, filter=""):
        populate_table(filter)

    def make_report(self):
        make_migration_report()        


def make_migration_report():
    migration_report.make_report()


def populate_table(filter):
    # no dynamo batch upsert, unforch...
    counter = 0
    start = time.time()
    for bnumber in bnumber_generator(filter):
        counter = counter + 1
        if counter % 100 == 0:
            t = int(round(time.time() - start))
            print("...{0} b numbers registered after {1} seconds..".format(counter, t))
        status_table.record_activity(bnumber, "registered")
    t = int(round(time.time() - start))
    print("Registered {0} b numbers for filter {1} in {2} s".format(counter, filter, t))


def get_min_bag_date():
    min_bag_date = datetime.datetime(2018, 12, 1)
    try:
        min_bag_date = dateutil.parser.parse(settings.MINIMUM_BAG_DATE)
    except ValueError:
        print("no parseable bag cutoff date in " + settings.MINIMUM_BAG_DATE)
    return min_bag_date


def update_bag_and_ingest_status(delay, filter):
    table = status_table.get_table()
    no_ingest = {
        "id": "-",
        "status": {"id": "no-ingest"},
        "events": [{"createdDate": "-"}],
    }

    print("[")

    for bnumber in bnumber_generator(filter):
        try:
            output = update_bag_and_ingest_status_bnumber(bnumber, table, no_ingest)
        except Exception as e:
            err_obj = {"ERROR": bnumber, "error": e}
            print(err_obj)
            raise
        print(json.dumps(output, default=json_default, indent=4))
        print(",")
        if delay > 0:
            time.sleep(delay)

    print("]")


def update_bag_and_ingest_status_bnumber(bnumber, table, no_ingest):
    bag_date = "-"
    bag_size = 0
    bag_error = "-"

    # check for bag
    bag_zip = aws.get_dropped_bag_info(bnumber)
    if bag_zip["exists"]:  # and bag_zip["last_modified"] > min_bag_date:
        bag_date = bag_zip["last_modified"].isoformat()
        bag_size = bag_zip["size"]
    else:
        error_obj = aws.get_error_for_b_number(bnumber)
        if error_obj is not None:
            message = error_obj["error"].splitlines()[-1]
            last_modified = error_obj["last_modified"]
            bag_error = "{0} {1}".format(last_modified, message)

    ingest = storage_api.get_ingest_for_identifier(bnumber)
    if ingest is None:
        ingest = no_ingest

    table.update_item(
        Key={"bnumber": bnumber},
        ExpressionAttributeValues={
            ":upd": now_as_string(),
            ":bdt": bag_date,
            ":bsz": bag_size,
            ":bge": bag_error,
            ":idt": ingest["events"][0]["createdDate"],
            ":iid": ingest["id"],
            ":ist": ingest["status"]["id"],
        },
        UpdateExpression="SET updated = :upd, bag_date = :bdt, bag_size = :bsz, mets_error = :bge, ingest_date = :idt, ingest_id = :iid, ingest_status = :ist",
    )

    return {
        "identifier": bnumber,
        "bag_zip": bag_zip,
        "mets_error": bag_error,
        "ingest": ingest,
    }


def update_dds_status_slow(delay, filter, force):
    table = status_table.get_table()

    print("[")

    for bnumber in bnumber_generator(filter):
        try:
            output = update_dds_status_bnumber(bnumber, table, force)
        except Exception as e:
            err_obj = {"ERROR": bnumber, "error": e}
            print(err_obj)
            raise
        print(json.dumps(output, default=json_default, indent=4))
        print(",")
        if delay > 0:
            time.sleep(delay)

    print("]")


def update_dds_status_bnumber(bnumber, table, force):

    status = table.get_item(Key={"bnumber": bnumber})["Item"]

    print()
    print(status)
    print()

    package_date = status.get("package_date", "0")
    # "texts_expected" - the number of cached TextObjs there should be in DDS
    # "texts_cached" - the number actually present
    # "dlcs_mismatch" - the sum across all manifestations of assets with sync issues
    texts_expected = status.get("texts_expected", -1)
    texts_cached = status.get("texts_cached", 0)
    dlcs_mismatch = status.get("dlcs_mismatch", -1)

    dds_package_date = dds.get_package_file_modified(bnumber)

    print(dds_package_date)
    print("-")
    if dds_package_date is None:
        package_date = "0"
    else:
        if force or dds_package_date > package_date:
            # there is a package, so we can ask this...
            # but don't ask it unless we really have to, as it's expensive
            texts_expected, texts_cached = dds.get_text_info(bnumber)
            dlcs_mismatch = dds.get_dlcs_mismatch(bnumber)

        package_date = dds_package_date

    table.update_item(
        Key={"bnumber": bnumber},
        ExpressionAttributeValues={
            ":pkg": package_date,
            ":tex": texts_expected,
            ":tch": texts_cached,
            ":dmm": dlcs_mismatch,
        },
        UpdateExpression="SET package_date = :pkg, texts_expected = :tex, texts_cached = :tch, dlcs_mismatch = :dmm",
    )

    return {
        "identifier": bnumber,
        "package_date": package_date,
        "texts_expected": texts_expected,
        "texts_cached": texts_cached,
        "dlcs_mismatch": dlcs_mismatch,
    }


def batch(bnumbers):
    chunk = []
    counter = 1
    for b in bnumbers:
        chunk.append(b)
        if counter == 25:
            yield chunk
            chunk = []
            counter = 1
        else:
            counter = counter + 1
    if len(chunk) > 0:
        yield chunk


def empty_item(bnumber):
    return {
        "bnumber": bnumber,
        "bag_date": None,
        "ingest_started": None,
        "ingest_id": None,
        "ingest_status": None,
    }


def do_ingest(delay, filter):
    batch_id = str(uuid.uuid4())
    print("[")
    for bnumber in bnumber_generator(filter):
        ingest = storage_api.ingest(bnumber)
        status_table.record_data(
            bnumber,
            {
                "ingest_start": status_table.activity_timestamp(),
                "ingest_batch_id": batch_id,
                "ingest_filter": filter,
            },
        )
        print(json.dumps(ingest, default=json_default, indent=4))
        print(",")
        if delay > 0:
            time.sleep(delay)
    print('"end"')
    print("]")


def call_dds(delay, filter, total, after):
    table = status_table.get_table()
    if filter is not None:
        bnumber_source = bnumber_generator(filter)
    else:
        bnumber_source = get_uncalled_bnumbers(table, total, after)
        
    for bnumber in bnumber_source:
        print("[")
        url = settings.DDS_GOOBI_NOTIFICATION.format(bnumber)
        print("requesting: {0}".format(url))
        r = requests.get(url)
        j = r.json()
        print(json.dumps(j, indent=4))
        print(",")

        # now update the dynamodb record
        table.update_item(
            Key={"bnumber": bnumber},
            ExpressionAttributeValues={":dc": now_as_string()},
            UpdateExpression="SET dds_called = :dc",
        )

        if delay > 0:
            time.sleep(delay)

    print('{ "finished": "' + now_as_string() + '" }')
    print("]")


def get_uncalled_bnumbers(table, total, after):
    returned = 0
    for item in table.all_items():
        ingest_date = item.get("ingest_date", "0")
        if ingest_date > after:
            dds_called = item.get("dds_called", "0")
            if dds_called < after:
                returned = returned + 1
                yield item["bnumber"]
                if returned == total:
                    break


def now_as_string():
    return datetime.datetime.now().isoformat()


if __name__ == "__main__":
    fire.Fire(MigrationTool)
