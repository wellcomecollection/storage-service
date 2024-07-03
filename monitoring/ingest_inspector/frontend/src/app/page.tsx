"use client";

import RecentIngests from "./components/RecentIngests";
import Form from "./components/Form";
import { APIErrors, useGetIngest } from "@/app/hooks/useGetIngest";
import cx from "classnames";
import Ingest from "@/app/components/Ingest";
import { useSearchParams } from "next/navigation";
import IngestError from "@/app/components/IngestError";

const HomePage = () => {
  const searchParams = useSearchParams();
  const ingestId = searchParams.get("ingestId") || "";
  const { data, isLoading, error } = useGetIngest(ingestId);

  return (
    <div className={cx(data && `status-${data.status.id}`)}>
      <Form defaultIngestId={ingestId} />
      <hr />
      <div className="content mt-6">
        {data && <Ingest ingestData={data} />}
        {error && <IngestError error={error} ingestId={ingestId} />}
        {!ingestId && <RecentIngests />}
      </div>
    </div>
  );
};

export default HomePage;
