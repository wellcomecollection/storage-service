import Link from "next/link";
import {IngestData} from "@/app/types";
import cx from "classnames";
import {useEffect, useState} from "react";

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
            {recentIngests.length > 0 && <h2 className="font-bold text-xl mb-2">Recently viewed ingests</h2>}
            <ul className="flex flex-wrap gap-4">
                {recentIngests.map((ingestData) => (
                    <li key={ingestData.id} className="w-full basis-full lg:basis-[calc(50%-16px)]">
                        <div
                            className={cx("w-full h-fit flex relative bg-[#EDECE3]", `status-${ingestData.status.id}`)}>
                            <Link href={`?ingestId=${ingestData.id}`} className="w-full h-full no-underline">
                                <div className="status-bg w-2 h-full absolute"/>
                                <div className="p-4">
                                    <div>{ingestData.id}</div>
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
