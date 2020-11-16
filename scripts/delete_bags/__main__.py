import click


def delete_ingest(ingest_id):
    print(ingest_id)


@click.command()
@click.argument("ingest_id", required=True)
def main(ingest_id):
    delete_ingest(ingest_id)


if __name__ == "__main__":
    main()
