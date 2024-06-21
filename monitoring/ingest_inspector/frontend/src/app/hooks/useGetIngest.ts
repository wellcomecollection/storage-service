import useSWR from "swr";
import {IngestInspectorApiResponse} from "@/app/types";
import {useEffect} from "react";
import {storeNewIngest} from "@/app/components/RecentIngests";
import NProgress from "nprogress";

const BASE_API_URL =
    "https://xkgpnijmy5.execute-api.eu-west-1.amazonaws.com/v1/ingest";

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


export const useGetIngest = (ingestId: string | null) => {
    const {data, isLoading, error} = useSWR<IngestInspectorApiResponse>(ingestId, getIngest, {revalidateOnFocus: false});

    useEffect(() => {
        if (data) {
            storeNewIngest(data.ingest);
        }
    }, [data]);

    useEffect(() => {
        NProgress.configure({
            showSpinner: false,
            parent: ".loading-indicator-wrapper",
        });
    }, []);

    return {
        data: data,
        isLoading: isLoading,
        error: error
    }
}
