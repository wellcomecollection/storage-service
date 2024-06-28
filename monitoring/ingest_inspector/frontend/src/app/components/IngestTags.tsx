import {IngestData} from "@/app/types";

export const IngestTags = ({ingestData}: {ingestData: IngestData}) => (
    <div className="flex font-medium gap-1 text-sm">
        <div className={`px-1 api-${ingestData.environment} capitalize status-color`}>{ingestData.status.id}</div>
        <div
            className={`px-1 api-${ingestData.environment} !border-black capitalize !bg-black !text-white`}>{ingestData.environment}</div>
    </div>
)

export default IngestTags;
