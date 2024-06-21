'use client';

import RecentIngests from "./components/RecentIngests";
import Form from "./components/Form";
import {useGetIngest} from "@/app/hooks/useGetIngest";
import cx from "classnames";
import Ingest from "@/app/components/Ingest";
import {useSearchParams} from "next/navigation";

const KIBANA_ERROR_URL =
    "https://logging.wellcomecollection.org/app/kibana#/discover?_g=()&_a=(columns:!(log),index:'6db79190-8556-11ea-8b79-41cfdb8d9024',interval:auto,query:(language:kuery,query:'service_name%20:%20%22*-ingests-service%22'),sort:!(!('@timestamp',desc)))";


const HomePage = () => {
    const searchParams = useSearchParams();
    const ingestId = searchParams.get("ingestId");
    const {data, isLoading, error} = useGetIngest(ingestId);

    return (
        <div className={cx(`status-${data?.ingest.status.id}`)}>
            <Form ingestId={ingestId as string}/>
            <hr/>
            <div className="content mt-6">
                {data && (
                    <Ingest ingestData={data.ingest} environment={data.environment}/>
                )}
                {error && (
                    <div>
                        {error.message}
                        <p>
                            Developers can <a href={KIBANA_ERROR_URL}>look at API logs</a>{" "}
                            in Kibana.
                        </p>
                    </div>
                )}
            </div>
            {!ingestId && <RecentIngests />}
        </div>
    );
};


export default HomePage;
