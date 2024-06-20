import { getRecentIngests } from "./utils";

export const RecentIngests = () => {
  const recentIngests = getRecentIngests();

  return (
    <>
      <p>Recently viewed ingests:</p>
      <ul>
        {recentIngests.map((ingestData) => (
          <li key={ingestData.ingestId}>
            <a href={`/${ingestData.ingestId}`}>{ingestData.ingestId}</a>{" "}
            &ndash; {ingestData.space}/{ingestData.externalIdentifier}
          </li>
        ))}
      </ul>
    </>
  );
};

export default RecentIngests;
