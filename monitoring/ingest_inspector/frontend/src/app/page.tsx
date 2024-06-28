'use client';

import RecentIngests from "./components/RecentIngests";
import Form from "./components/Form";
import {APIErrors, useGetIngest} from "@/app/hooks/useGetIngest";
import cx from "classnames";
import Ingest from "@/app/components/Ingest";
import {useSearchParams} from "next/navigation";

const KIBANA_ERROR_URL =
    "https://logging.wellcomecollection.org/app/kibana#/discover?_g=()&_a=(columns:!(log),index:'6db79190-8556-11ea-8b79-41cfdb8d9024',interval:auto,query:(language:kuery,query:'service_name%20:%20%22*-ingests-service%22'),sort:!(!('@timestamp',desc)))";


const HomePage = () => {
    const searchParams = useSearchParams();
    const ingestId = searchParams.get("ingestId") || "";
    const {data, isLoading, error} = useGetIngest(ingestId);

    return (
        <div className={cx(data && `status-${data.status.id}`)}>
            <Form defaultIngestId={ingestId}/>
            <hr/>
            <div className="content mt-6">
                {data && (
                    <Ingest ingestData={data} environment={data.environment}/>
                )}
                {error && (
                    <div className="mt-12">
                        {error.message === APIErrors.INVALID_INGEST_ID && <h3 className="text-2xl">The ingest ID <strong>{ingestId}</strong> is not valid.</h3>}
                        {error.message === APIErrors.INGEST_NOT_FOUND && <h3 className="text-2xl">Could not find ingest <strong>{ingestId}</strong>.</h3>}
                        <p className="mt-4 text-lg">
                            Developers can <a href={KIBANA_ERROR_URL} target="_blank" rel="noreferrer">look at API logs</a>{" "}
                            in Kibana.
                        </p>
                    </div>
                )}
                {!ingestId && <RecentIngests />}
            </div>
        </div>
    );
};


export default HomePage;
