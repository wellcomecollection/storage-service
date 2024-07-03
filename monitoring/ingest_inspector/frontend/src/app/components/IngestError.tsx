import { APIErrors } from "@/app/hooks/useGetIngest";

const KIBANA_ERROR_URL =
  "https://logging.wellcomecollection.org/app/kibana#/discover?_g=()&_a=(columns:!(log),index:'6db79190-8556-11ea-8b79-41cfdb8d9024',interval:auto,query:(language:kuery,query:'service_name%20:%20%22*-ingests-service%22'),sort:!(!('@timestamp',desc)))";

type IngestErrorProps = {
  ingestId: string;
  error: Error;
};

const IngestError = ({ ingestId, error }: IngestErrorProps) => {
  let errorMessage;

  if (error.message === APIErrors.INVALID_INGEST_ID) {
    errorMessage = (
      <>
        The ingest ID <strong>{ingestId}</strong> is not valid.
      </>
    );
  } else if (error.message === APIErrors.INGEST_NOT_FOUND) {
    errorMessage = (
      <>
        Could not find ingest <strong>{ingestId}</strong>.
      </>
    );
  } else {
    errorMessage = (
      <>
        Something went wrong while looking up ingest <strong>{ingestId}</strong>
        .
      </>
    );
  }

  return (
    <div className="mt-12">
      <h3 className="text-2xl">{errorMessage}</h3>
      <p className="mt-4 text-lg">
        Developers can{" "}
        <a href={KIBANA_ERROR_URL} target="_blank" rel="noreferrer">
          look at API logs
        </a>{" "}
        in Kibana.
      </p>
    </div>
  );
};

export default IngestError;
