# Miro Migration

Scripts to assist in migrating Miro images from S3 into the Wellcome Storage Service.

This folder contains of a `docker-compose.yml` intended to help run Elasticsearch & Kibana with migration code. 

## Running

If you run `docker-compose up` the `create-decisions-index` command will be run on the migration container built from `Dockerfile`.

Kibana will be available at http://localhost:5601 to explore the data.

It's intended that commands will be run in the following order:

- `create-decisions-index`: Build an Elasticsearch index of locations from S3, along with a reference for where that asset was stored after miro decommisioning.
- `create-chunks-index`: Build an Elasticsearch index of "chunks" indicating which files should be in which transfer package.
= `transfer-package-chunks`: Create and upload transfer packages to Archivematica, recording progress in the chunks index.

## Developing

You can modify the `docker-compose.yml` to run any command you wish on the migration container.

To rebuild the migration container image you will need to run `docker-compose build migration`.

You can run Elasticsearch only with `docker-compose run elasticsearch` if you wish to develop outside of docker.
