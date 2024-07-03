import useSWR from "swr";
import { useEffect } from "react";
import { storeNewIngest } from "@/app/components/RecentIngests";
import NProgress from "nprogress";
import { IngestData } from "@/app/types";

const BASE_API_URL =
  "https://gzz79crkhl.execute-api.eu-west-1.amazonaws.com/v1/ingest";

export const APIErrors = {
  INVALID_INGEST_ID: "Invalid ingest ID.",
  INGEST_NOT_FOUND: "Ingest not found.",
};

const getIngest = async (ingestId: string): Promise<IngestData> => {
  const response = await fetch(`${BASE_API_URL}/${ingestId}`);
  const content = await response.json();

  if (response.status === 200) {
    return content;
  }

  throw Error(content.message);
};

export const useGetIngest = (ingestId: string | null) => {
  const { data, isLoading, error } = useSWR<IngestData>(ingestId, getIngest, {
    revalidateOnFocus: false,
  });

  useEffect(() => {
    if (data) {
      storeNewIngest(data);
    }
  }, [data]);

  // Configure loading progress bar on mount
  useEffect(() => {
    NProgress.configure({
      showSpinner: false,
      parent: ".loading-indicator-wrapper",
    });
  }, []);

  useEffect(() => {
    if (isLoading) {
      NProgress.start();
    }
    if (!isLoading) {
      NProgress.done();
    }
  }, [isLoading]);

  return {
    data: data,
    isLoading: isLoading,
    error: error,
  };
};
