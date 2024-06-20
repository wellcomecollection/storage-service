import { useParams, useNavigate } from "react-router-dom";
import Ingest from "./Ingest";
import cx from "classnames";
import useSWR from "swr";
import RecentIngests from "./RecentIngests";
import { IngestInspectorApiResponse } from "./types";
import { useEffect } from "react";
import { storeNewIngest } from "./utils";

const BASE_API_URL =
  "https://xkgpnijmy5.execute-api.eu-west-1.amazonaws.com/v1/ingest";
const KIBANA_ERROR_URL =
  "https://logging.wellcomecollection.org/app/kibana#/discover?_g=()&_a=(columns:!(log),index:'6db79190-8556-11ea-8b79-41cfdb8d9024',interval:auto,query:(language:kuery,query:'service_name%20:%20%22*-ingests-service%22'),sort:!(!('@timestamp',desc)))";
const getIngest = async (
  ingestId: string,
): Promise<IngestInspectorApiResponse> => {
  const response = await fetch(`${BASE_API_URL}/${ingestId}`);
  const content = await response.json();

  if (response.status === 200) {
    return content;
  }

  throw Error(content.message);
};

const App = () => {
  const { ingestId } = useParams();
  const navigate = useNavigate();

  const { data, isLoading, error } = useSWR<IngestInspectorApiResponse>(ingestId, getIngest);

  const onSubmit = (e) => {
    e.preventDefault();
    const data = new FormData(e.target);
    const ingestId: string = [...data.entries()][0][1] as string;

    if (ingestId?.length > 0) {
      navigate(`/${ingestId}`);
    }
  };

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

  return (
    <div>
      <header className={cx(`status-${data?.ingest.status.id}`)}>
        <div className="content">
          <a href="/">wellcome ingest inspector</a>
        </div>
      </header>
      <main>
        <div
          className="content"
          style={{ marginBottom: "2em", marginTop: "1em" }}
        >
          <form onSubmit={onSubmit}>
            <div className="input-wrapper">
              <label htmlFor="ingest-id">Ingest ID</label>
              <input
                defaultValue={ingestId}
                type="text"
                name="ingest-id"
                placeholder="123e4567-e89b-12d3-a456-426655440000"
                autoFocus
                spellCheck="false"
                size={36}
                className={cx(`status-${data?.ingest.status.id}`)}
              />
            </div>
            <button
              type="submit"
              className={cx(`status-${data?.ingest.status.id}`)}
            >
              Look up ingest
            </button>
          </form>
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
          {!isLoading && !data && !error && <RecentIngests />}
        </div>
      </main>
      <footer>
        <div className="content">
          <p>
            made with{" "}
            <span className="heart {% if ingest %}status-{{ ingest.status.id }}{% endif %}">
              ♥
            </span>{" "}
            • source on{" "}
            <a href="https://github.com/wellcomecollection/ingest-inspector">
              GitHub
            </a>
          </p>
        </div>
      </footer>
    </div>
  );
};

export default App;
