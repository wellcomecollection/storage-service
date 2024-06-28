import { IngestData } from "@/app/types";
import cx from "classnames";

export const IngestTags = ({ ingestData }: { ingestData: IngestData }) => (
  <div className="flex font-medium gap-1 text-sm">
    <div
      className={cx("px-1 capitalize border-[3px] border-black", {
        "bg-black text-white": ingestData.environment === "production",
      })}
    >
      {ingestData.environment}
    </div>
    <div
      className={cx("px-1 capitalize border-[3px] text-white", {
        "status-color": ingestData.environment === "staging",
        "status-bg": ingestData.environment === "production",
      })}
    >
      {ingestData.status.id}
    </div>
  </div>
);

export default IngestTags;
