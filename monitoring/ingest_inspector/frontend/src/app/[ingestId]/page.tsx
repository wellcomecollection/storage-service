'use client';

import Ingest from "../components/Ingest";
import cx from "classnames";
import useSWR from "swr";
import Form from "../components/Form";
import { IngestInspectorApiResponse } from "../types";
import { useEffect } from "react";
import { storeNewIngest } from "../utils";
import { useParams } from "next/navigation";
import NProgress from "nprogress";

const BASE_API_URL =
  "https://xkgpnijmy5.execute-api.eu-west-1.amazonaws.com/v1/ingest";
const KIBANA_ERROR_URL =
  "https://logging.wellcomecollection.org/app/kibana#/discover?_g=()&_a=(columns:!(log),index:'6db79190-8556-11ea-8b79-41cfdb8d9024',interval:auto,query:(language:kuery,query:'service_name%20:%20%22*-ingests-service%22'),sort:!(!('@timestamp',desc)))";


const getIngest = async (
  ingestId: string,
): Promise<IngestInspectorApiResponse> => {
  NProgress.start();
  const response = await fetch(`${BASE_API_URL}/${ingestId}`);
  const content = await response.json();
  NProgress.done();

  if (response.status === 200) {
    return content;
  }

  throw Error(content.message);
};

const IngestPage = () => {
  const {ingestId} = useParams();
  const { data, isLoading, error } = useSWR<IngestInspectorApiResponse>(ingestId, getIngest, {revalidateOnFocus: false});

  useEffect(() => {
    if (data) {
      const ingest = data.ingest;
      storeNewIngest(
        ingestId,
        ingest.space.id,
        ingest.bag.info.externalIdentifier,
      );
    }
  }, [data]);

  useEffect(() => {
    NProgress.configure({
      showSpinner: false,
      parent: ".loading-indicator-wrapper",
    });
  }, []);


  return (
    <div>
      <main>
        <div
          className="content"
          style={{ marginBottom: "2em", marginTop: "1em" }}
        >
          <Form/>
        </div>
        {!isLoading && <hr />}
        <div className="content">
          {isLoading && !data && !error && <div>Loading</div>}
          {data && (
            <Ingest ingestData={data.ingest} environment={data.environment} />
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
      </main>
    </div>
  );
};

export default IngestPage;