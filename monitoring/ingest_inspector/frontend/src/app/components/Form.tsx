import { useRouter } from "next/navigation";
import cx from 'classnames';

export const Form = ({ingestId, data}) => {
  const router = useRouter();

  const onSubmit = (e) => {
    e.preventDefault();
    const data = new FormData(e.target);
    const ingestId: string = [...data.entries()][0][1] as string;

    if (ingestId?.length > 0) {
      router.push(`/${ingestId}`);
    }
  };


  return (
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
        className={cx(`status-${data?.ingest.status.id}`)}
      />
    </div>
    <button
      type="submit"
      className={cx(`status-${data?.ingest.status.id}`)}
    >
      Look up ingest
    </button>
  </form>
  );
};

export default Form;
