import { localiseDateString, updateDelta } from "../utils";
import { IngestData } from "../types";
import cx from "classnames";

const STAGING_PRIMARY_BUCKET = "wellcomecollection-storage-staging";
const STAGING_GLACIER_BUCKET =
  "wellcomecollection-storage-staging-replica-ireland";
const PRODUCTION_PRIMARY_BUCKET = "wellcomecollection-storage";
const PRODUCTION_GLACIER_BUCKET = "wellcomecollection-storage-replica-ireland";

const getOrdinal = (n: number) => {
  // Returns the ordinal value of n, e.g. 1st, 2nd, 3rd
  // From https://leancrew.com/all-this/2020/06/ordinals-in-python/

  const suffix = ["th", "st", "nd", "rd"].concat(Array(10).fill("th"));
  const v = n % 100;

  if (v > 13) {
    return `${n}${suffix[v % 10]}`;
  }

  return `${n}${suffix[v]}`;
};

type IngestProps = {
  ingestData: IngestData;
  environment: string;
};

const Ingest = ({ ingestData, environment }: IngestProps) => {
  const status = ingestData.status.id;
  const version = ingestData.bag.info.version;

  const glacierBucket =
    environment === "production"
      ? PRODUCTION_GLACIER_BUCKET
      : STAGING_GLACIER_BUCKET;
  const primaryBucket =
    environment === "production"
      ? PRODUCTION_PRIMARY_BUCKET
      : STAGING_PRIMARY_BUCKET;

  const space = ingestData.space.id;

  const path = `${space}/${ingestData.bag.info.externalIdentifier}/${ingestData.bag.info.version}`;

  return (
    <>
      <p>
        Found ingest in the <strong>{environment}</strong> API:
      </p>
      <div className="card">
        <div className={`card-header api-${environment}`}>
          {ingestData.id}: {status}
        </div>
        <div className="card-body">
          <div className="ingest-data">
            <div className="label">source location:</div>
            <div className="value">
              <a href={ingestData.s3Url}>{ingestData.displayS3Url}</a>
            </div>
            <div className="label">storage space:</div>
            <div className="value">{space}</div>

            <div className="label">external identifier:</div>
            <div className="value">
              {ingestData.bag.info.externalIdentifier}
            </div>

            <div className="label">version:</div>
            <div className="value">{version || "none assigned"}</div>

            {/*If the ingest succeeded, we can link to the bag in S3.*/}
            {status === "succeeded" && (
              <>
                <div className="label">bag locations:</div>
                <div className="value">
                  <a
                    href={`https://s3.console.aws.amazon.com/s3/buckets/${primaryBucket}/${path}/?region=eu-west-1&tab=overview`}
                  >
                    s3://{primaryBucket}/{path}
                  </a>
                  <br />
                  <a
                    href={`https://s3.console.aws.amazon.com/s3/buckets/${glacierBucket}/${path}/?region=eu-west-1&tab=overview`}
                  >
                    s3://{glacierBucket}/{path}
                  </a>
                </div>
              </>
            )}
            <div className="label">created date:</div>
            <div className="value timestamp" title={ingestData.createdDate}>
              {localiseDateString(ingestData.createdDate)}
            </div>

            <div className="label">last update:</div>
            <div className="value timestamp" title={ingestData.lastUpdatedDate}>
              {localiseDateString(ingestData.lastUpdatedDate) +
                updateDelta(ingestData.lastUpdatedDate)}
            </div>

            {ingestData.callback != null && (
              <>
                <div
                  className={`label callback--${ingestData.callback.status.id}`}
                >
                  callback status:
                </div>
                <div
                  className={`value callback--${ingestData.callback.status.id}`}
                >
                  {ingestData.callback.status.id === "processing"
                    ? "pending"
                    : ingestData.callback.status.id}
                </div>
              </>
            )}
            <div className="label">events:</div>
            <div className="value">
              <ul>
                {ingestData.events.map((event) => (
                  <li title={event.createdDate} key={event.createdDate}>
                    <span className={cx({"font-semibold": event.description.includes("failed")})}>{event.description}</span>
                    {event._repeated && (
                      <span className="count">
                        {" "}
                        ({getOrdinal(event._count)} attempt /{" "}
                        <a href={event.kibanaUrl}>dev logs</a>)
                      </span>
                    )}
                    {!event._repeated &&
                      (event.description.includes("failed") ||
                        event._is_unmatched_start) && (
                        <>
                          {" "}
                          (<a href={event.kibanaUrl}>dev logs</a>)
                        </>
                      )}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      </div>
    </>
  );
};

export default Ingest;
