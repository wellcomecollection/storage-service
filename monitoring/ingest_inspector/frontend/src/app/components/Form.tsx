import { useRouter } from "next/navigation";
import { FormEvent, useEffect, useState } from "react";

type FormProps = {
  defaultIngestId: string;
};

export const Form = ({ defaultIngestId }: FormProps) => {
  const [ingestId, setIngestId] = useState<string>(defaultIngestId);
  const router = useRouter();

  useEffect(() => {
    setIngestId(defaultIngestId);
  }, [defaultIngestId]);

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
      <form onSubmit={onSubmit} className="flex items-end">
        <div className="w-full mr-[10px]">
          <label htmlFor="ingest-id">Ingest ID</label>
          <input
            type="text"
            name="ingest-id"
            placeholder="123e4567-e89b-12d3-a456-426655440000"
            autoFocus
            spellCheck="false"
            value={ingestId}
            onChange={(e) => setIngestId(e.target.value)}
          />
        </div>
        <button type="submit" className="status-bg hover:underline">
          Look up ingest
        </button>
      </form>
    </div>
  );
};

export default Form;
