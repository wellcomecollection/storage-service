class CannotGetMiroChunk(Exception):
    pass


def choose_miro_chunk(miro_id):
    """
    Decides the chunk that a Miro ID belongs to.
    """
    raise CannotGetMiroChunk(miro_id)


if __name__ == "__main__":
    from miro_ids import parse_miro_id, IgnoreMiroIDError
    from s3 import list_s3_objects_from

    for s3_obj in list_s3_objects_from(
        bucket="wellcomecollection-assets-workingstorage",
        prefix="miro/Wellcome_Images_Archive",
    ):
        try:
            miro_id = parse_miro_id(s3_key=s3_obj["Key"])
        except IgnoreMiroIDError:
            continue
        print(miro_id)
        print(s3_obj)
        miro_chunk = choose_miro_chunk(miro_id)
        print(miro_chunk)
        break
