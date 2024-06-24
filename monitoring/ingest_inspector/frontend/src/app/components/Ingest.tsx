import {localiseDateString, updateDelta} from "../utils";
import {IngestData} from "../types";
import cx from "classnames";
import IngestEvent from "@/app/components/IngestEvent";

const STAGING_PRIMARY_BUCKET = "wellcomecollection-storage-staging";
const STAGING_GLACIER_BUCKET =
    "wellcomecollection-storage-staging-replica-ireland";
const PRODUCTION_PRIMARY_BUCKET = "wellcomecollection-storage";
const PRODUCTION_GLACIER_BUCKET = "wellcomecollection-storage-replica-ireland";

const getS3Url = (bucket: string, path: string) => {
    return `https://s3.console.aws.amazon.com/s3/buckets/${bucket}/${path}/?region=eu-west-1&tab=overview`
}

const IngestFormattedDateItem = ({label, date}) => (
    <>
        <dt>{label}</dt>
        <dd className="timestamp" title={date}>
            {localiseDateString(date)}
        </dd>
    </>
)

type IngestProps = {
    ingestData: IngestData;
    environment: string;
};

const Ingest = ({ingestData, environment}: IngestProps) => {
    const glacierBucket =
        environment === "production"
            ? PRODUCTION_GLACIER_BUCKET
            : STAGING_GLACIER_BUCKET;
    const primaryBucket =
        environment === "production"
            ? PRODUCTION_PRIMARY_BUCKET
            : STAGING_PRIMARY_BUCKET;

    const status = ingestData.status.id;
    const version = ingestData.bag.info.version;
    const space = ingestData.space.id;
    const path = `${space}/${ingestData.bag.info.externalIdentifier}/${ingestData.bag.info.version}`;
    const callbackStatus = ingestData.callback?.status.id;

    return (
        <>
            <p className="text-lg">
                Found ingest in the <strong>{environment}</strong> API:
            </p>
            <div className="card mt-3">
                <div className={`card-header api-${environment}`}>
                    {ingestData.id}: {status}
                </div>
                <dl className="ingest-data">
                    <dt>source location:</dt>
                    <dd>
                        <a href={ingestData.s3Url} target="_blank" rel="noreferrer">{ingestData.displayS3Url}</a>
                    </dd>

                    <dt>storage space:</dt>
                    <dd>{space}</dd>

                    <dt>external identifier:</dt>
                    <dd>{ingestData.bag.info.externalIdentifier}</dd>

                    <dt>version:</dt>
                    <dd>{version || "none assigned"}</dd>

                    {/*If the ingest succeeded, we can link to the bag in S3.*/}
                    {status === "succeeded" && (
                        <>
                            <dt>bag locations:</dt>
                            <dd>
                                <a href={getS3Url(primaryBucket, path)}>s3://{primaryBucket}/{path}</a>
                                <br/>
                                <a href={getS3Url(glacierBucket, path)}>s3://{glacierBucket}/{path}</a>
                            </dd>
                        </>
                    )}

                    <IngestFormattedDateItem label="created date:" date={ingestData.createdDate}/>
                    <IngestFormattedDateItem label="last update:" date={ingestData.lastUpdatedDate}/>

                    {callbackStatus && (
                        <>
                            <dt className={`callback--${callbackStatus}`}>
                                callback status:
                            </dt>
                            <dd className={`callback--${callbackStatus}`}>
                                {callbackStatus === "processing" ? "pending" : callbackStatus}
                            </dd>
                        </>
                    )}
                    <dt>events:</dt>
                    <dd>
                        <ul>
                            {ingestData.events.map((event) => (<IngestEvent event={event}/>))}
                        </ul>
                    </dd>
                </dl>
            </div>
        </>
    );
};

export default Ingest;
