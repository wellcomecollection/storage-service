const dateFormatter = new Intl.DateTimeFormat([], {
    year: "numeric",
    month: "long",
    day: "numeric",
    weekday: "short",
});

const timeFormatter = new Intl.DateTimeFormat([], {
    hour: "numeric",
    minute: "numeric",
    timeZoneName: "short",
});

// Localise a date for the current timezone
export const localiseDateString = (ds: string) => {
    const today = new Date();
    const yesterday = new Date().setDate(new Date().getDate() - 1);

    const date = new Date(Date.parse(ds));

    const isGerman = timeFormatter.resolvedOptions()["locale"] == "de-DE";

    if (dateFormatter.format(date) == dateFormatter.format(today)) {
        if (isGerman) {
            return "heute @ " + timeFormatter.format(date);
        } else {
            return "today @ " + timeFormatter.format(date);
        }
    } else if (dateFormatter.format(date) == dateFormatter.format(yesterday)) {
        if (isGerman) {
            return "gestern @ " + timeFormatter.format(date);
        } else {
            return "yesterday @ " + timeFormatter.format(date);
        }
    } else {
        return dateFormatter.format(date) + " @ " + timeFormatter.format(date);
    }
};


export const updateDelta = (ds: string) => {
    const today = new Date();
    const date = new Date(Date.parse(ds));

    const delta = today.getTime() - date.getTime();
    const deltaSeconds = Math.floor(delta / 1000);

    const isGerman = timeFormatter.resolvedOptions()["locale"] == "de-DE";

    let result;

    if (isGerman) {
        if (deltaSeconds == 1) {
            result = "vor 1 Sekunde";
        } else if (deltaSeconds < 60) {
            result = "vor " + deltaSeconds + " Sekunde";
        } else if (deltaSeconds < 2 * 60) {
            result = "vor 1 Minute";
        } else if (deltaSeconds < 60 * 60) {
            result = "vor " + Math.floor(deltaSeconds / 60) + " Minuten";
        } else {
            result = "";
        }
    } else {
        if (deltaSeconds < 5) {
            result = "just now";
        } else if (deltaSeconds < 60) {
            result = deltaSeconds + " seconds ago";
        } else if (deltaSeconds < 2 * 60) {
            result = "1 minute ago";
        } else if (deltaSeconds < 60 * 60) {
            result = Math.floor(deltaSeconds / 60) + " minutes ago";
        } else {
            result = "";
        }
    }

    if (result !== "") {
        return " (" + result + ")";
    } else {
        return "";
    }
};

type IngestFormattedDateItemProps = {
    label: string;
    date: string;
    includeDelta: boolean;
}

const IngestFormattedDateItem = ({label, date, includeDelta}: IngestFormattedDateItemProps) => (
    <>
        <dt>{label}</dt>
        <dd className="timestamp w-fit" title={date}>
            {localiseDateString(date)}
            {includeDelta && ` ${updateDelta(date)}`}
        </dd>
    </>
)

export default IngestFormattedDateItem;
