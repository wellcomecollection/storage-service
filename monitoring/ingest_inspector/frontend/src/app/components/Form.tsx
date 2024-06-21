import {useRouter} from "next/navigation";
import {FormEvent} from "react";

type FontProps = {
    ingestId?: string;
}

export const Form = ({ingestId}: FontProps) => {
    const router = useRouter();

    const onSubmit = (e: FormEvent) => {
        e.preventDefault();
        const data = new FormData(e.target as HTMLFormElement);
        const ingestId = data.get("ingest-id") as string;

        if (ingestId?.length > 0) {
            router.push(`/?ingestId=${ingestId}`);
        }
    };

    return (
        <div className="content my-8">
            <form onSubmit={onSubmit}>
                <div className="input-wrapper">
                    <label htmlFor="ingest-id">Ingest ID</label>
                    <input
                        defaultValue={ingestId}
                        type="text"
                        name="ingest-id"
                        placeholder="123e4567-e89b-12d3-a456-426655440000"
                        autoFocus
                        spellCheck="false"
                        size={36}
                    />
                </div>
                <button type="submit" className="status-bg">
                    Look up ingest
                </button>
            </form>
        </div>
    );
};

export default Form;
