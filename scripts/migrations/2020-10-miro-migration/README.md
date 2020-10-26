# Miro Migration

Scripts to assist in migrating Miro images from S3 into the Wellcome Storage Service.

This folder contains of a `docker-compose.yml` intended to help run Elasticsearch & Kibana with migration code. 

## Running

If you run `docker-compose up` the `create-files-index` command will be run on the migration container built from `Dockerfile`.

Kibana will be available at http://localhost:5601 to explore the data.

## Developing

You can modify the `docker-compose.yml` to run any command you wish on the migration container.

To rebuild the migration container image you will need to run `docker-compose build migration`.

You can run Elasticsearch only with `docker-compose run elasticsearch` if you wish to develop outside of docker.
