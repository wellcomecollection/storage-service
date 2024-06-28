import {IngestData} from "../types";
import IngestEventItem from "@/app/components/IngestEventItem";
import IngestFormattedDateItem from "@/app/components/IngestFormattedDateItem";
import IngestTags from "@/app/components/IngestTags";

const STAGING_PRIMARY_BUCKET = "wellcomecollection-storage-staging";
const STAGING_GLACIER_BUCKET =
    "wellcomecollection-storage-staging-replica-ireland";
const PRODUCTION_PRIMARY_BUCKET = "wellcomecollection-storage";
const PRODUCTION_GLACIER_BUCKET = "wellcomecollection-storage-replica-ireland";

const getS3Url = (bucket: string, path: string) => {
    return `https://s3.console.aws.amazon.com/s3/buckets/${bucket}/${path}/?region=eu-west-1&tab=overview`
}

type IngestProps = {
    ingestData: IngestData;
};

const Ingest = ({ingestData}: IngestProps) => {
    const glacierBucket =
        ingestData.environment === "production"
            ? PRODUCTION_GLACIER_BUCKET
            : STAGING_GLACIER_BUCKET;
    const primaryBucket =
        ingestData.environment === "production"
            ? PRODUCTION_PRIMARY_BUCKET
            : STAGING_PRIMARY_BUCKET;

    const status = ingestData.status.id;
    const version = ingestData.bag.info.version;
    const space = ingestData.space.id;
    const path = `${space}/${ingestData.bag.info.externalIdentifier}/${ingestData.bag.info.version}`;
    const callbackStatus = ingestData.callback?.status.id;

    return (
        <div className="mt-3 bg-[#EDECE3] p-4 sm:p-8 card">
            <IngestTags ingestData={ingestData}/>
            <h2 className="text-3xl font-medium mt-2 mb-4">{ingestData.id}</h2>
            <dl className="ingest-data">
                <dt>Source location:</dt>
                <dd>
                    <a className="status-color" href={ingestData.s3Url} target="_blank" rel="noreferrer">{ingestData.displayS3Url}</a>
                </dd>

                <dt>Storage space:</dt>
                <dd>{space}</dd>

                <dt>External identifier:</dt>
                <dd>{ingestData.bag.info.externalIdentifier}</dd>

                <dt>Version:</dt>
                <dd>{version || "-"}</dd>

                {/*If the ingest succeeded, we can link to the bag in S3.*/}
                {status === "succeeded" && (
                    <>
                        <dt>Bag locations:</dt>
                        <dd>
                            <a className="status-color" href={getS3Url(primaryBucket, path)}>s3://{primaryBucket}/{path}</a>
                            <br/>
                            <a className="status-color" href={getS3Url(glacierBucket, path)}>s3://{glacierBucket}/{path}</a>
                        </dd>
                    </>
                )}

                <IngestFormattedDateItem label="Created:" date={ingestData.createdDate} includeDelta={false}/>
                <IngestFormattedDateItem label="Last updated:" date={ingestData.lastUpdatedDate} includeDelta/>

                {callbackStatus && (
                    <>
                        <dt className={`callback--${callbackStatus}`}>
                            Callback status:
                        </dt>
                        <dd className={`callback--${callbackStatus}`}>
                            {callbackStatus === "processing" ? "pending" : callbackStatus}
                        </dd>
                    </>
                )}
                <dt>Events:</dt>
                <dd>
                    <ul>
                        {ingestData.events.map((event) => (
                            <IngestEventItem key={event.createdDate} event={event}/>))}
                    </ul>
                </dd>
            </dl>
        </div>
    );
};

export default Ingest;
