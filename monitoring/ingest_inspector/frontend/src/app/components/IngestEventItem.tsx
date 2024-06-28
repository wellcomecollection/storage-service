import Link from "next/link";
import {IngestData} from "@/app/types";
import cx from "classnames";
import {useEffect, useState} from "react";
import Ingest from "@/app/components/Ingest";


const getOrdinal = (n: number) => {
    // Returns the ordinal value of n, e.g. 1st, 2nd, 3rd
    // From https://leancrew.com/all-this/2020/06/ordinals-in-python/

    const suffix = ["th", "st", "nd", "rd"].concat(Array(10).fill("th"));
    const v = n % 100;

    if (v > 13) {
        return `${n}${suffix[v % 10]}`;
    }

    return `${n}${suffix[v]}`;
};


const getAttemptCount = (event: IngestData["events"][0]) => {
    if (event._repeated) {
        return `${getOrdinal(event._count)} attempt`
    }
}

const IngestEventItem = ({event}: { event: IngestData["events"][0] }) => {
    const isFailed = event.description.includes("failed");
    const attemptCount = getAttemptCount(event);

    return (
        <li title={event.createdDate} key={event.createdDate}>
            <span className={cx({"font-semibold": isFailed})}>{event.description}</span>
            {(attemptCount || isFailed || event._is_unmatched_start) && (
                <span className={cx({"count": attemptCount})}>
                    {' '}(
                    {attemptCount && `${attemptCount} / `}
                    <a href={event.kibanaUrl} target="_blank" rel="noreferrer">dev logs</a>)
                </span>
            )}
        </li>
    )
}
export default IngestEventItem;
