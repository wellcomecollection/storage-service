import Link from "next/link";
import { getRecentIngests } from "../utils";

export const RecentIngests = () => {
  const recentIngests = getRecentIngests();

  return (
    <>
      <p>Recently viewed ingests:</p>
      <ul>
        {recentIngests.map((ingestData) => (
          <li key={ingestData.ingestId}>
            <Link href={`/${ingestData.ingestId}`}>{ingestData.ingestId}</Link>{" "}
            &ndash; {ingestData.space}/{ingestData.externalIdentifier}
          </li>
        ))}
      </ul>
    </>
  );
};

export default RecentIngests;
