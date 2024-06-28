import Link from "next/link";
import {IngestData} from "@/app/types";
import cx from "classnames";
import {useEffect, useState} from "react";
import IngestTags from "@/app/components/IngestTags";

export const getRecentIngests = (): Array<IngestData> => {
    const storedIngests = localStorage.getItem("recentIngests");

    if (storedIngests == null) {
        return [];
    }

    return JSON.parse(storedIngests);
};

export const storeNewIngest = (ingest: IngestData) => {
    const recentIngests = getRecentIngests();

    const otherIngests = recentIngests.filter((i) => i.id !== ingest.id);

    const newIngests = [ingest].concat(otherIngests);
    const ingestsToStore = newIngests.slice(0, 10);

    localStorage.setItem("recentIngests", JSON.stringify(ingestsToStore));
};

export const RecentIngests = () => {
    const [recentIngests, setRecentIngests] = useState<Array<IngestData>>([]);

    useEffect(() => {
        setRecentIngests(getRecentIngests());
    }, []);

    if (recentIngests.length === 0) {
        return null
    }

    return (
        <>
            {recentIngests.length > 0 && <h2 className="font-bold text-xl mb-6">Recently viewed ingests</h2>}
            <ul className="flex flex-wrap gap-6">
                {recentIngests.map((ingestData) => (
                    <li key={ingestData.id} className="w-full basis-full lg:basis-[calc(50%-12px)]">
                        <div className={cx("w-full h-fit flex relative bg-[#EDECE3] rounded-md", `status-${ingestData.status.id}`)}>
                            <Link href={`?ingestId=${ingestData.id}`} className="w-full h-full no-underline group">
                                <div className="p-4">
                                    <IngestTags ingestData={ingestData}/>
                                    <h3 className="font-semibold text-xl mt-3 group-hover:underline">{ingestData.id}</h3>
                                    <div>{ingestData.space.id}/{ingestData.bag.info.externalIdentifier}</div>
                                </div>
                            </Link>
                        </div>
                    </li>
                ))}
            </ul>
        </>
    );
};

export default RecentIngests;
