# Miro Migration

Scripts to assist in migrating Miro images from S3 into the Wellcome Storage Service.

This folder contains of a `docker-compose.yml` intended to help run Elasticsearch & Kibana with migration code. 

## Running

If you run `docker-compose up` the `create-decisions-index` command will be run on the migration container built from `Dockerfile`.

Kibana will be available at http://localhost:5601 to explore the data.

It's intended that commands will be run in the following order.

### Generate transfer packages

- `create-decisions-index`: Build an Elasticsearch index of locations from S3, along with a reference for where that asset was after miro decommissioning.
- `create-chunks-index`: Build an Elasticsearch index of "chunks" indicating which files should be in which transfer package.
  
  There are 3 different chunks indices that can be created:
  
  - `chunks`: Contains all the files that have miro IDs that we must register to DLCS to make those images available to users.
  - `chunks_no_miro_id`: Contains all the files that haven't been assigned a miro ID, and fall into no other category.
  - `chunks_movies_and_corporate`: Contains all the files that have been categorised as either corporate photography or movies.

- `transfer-package-chunks`: Create and upload transfer packages to S3, recording progress in the chunks index.

### Upload transfer packages

- `upload-transfer-packages`: Copies the transfer packages from one location in S3 into another where they are picked up Archivematica.

  As there are 3 indices, this should be invoked for each of them.

### Registering files with DLCS

- `create-files-index`: Build an Elasticsearch index of miro files from the storage service using reporting cluster.
- `create-registrations-index`: Build an Elasticsearch index of the necessary registrations that should take place with DLCS.
- `dlcs-send-registrations`: Traverse the registrations index, creating batches of files and requesting their registration from DLCS.
- `dlcs-update-registrations`: Traverse the registrations index, updating the status of registrations by querying DLCS.

## Developing

You can modify the `docker-compose.yml` to run any command you wish on the migration container.

To rebuild the migration container image you will need to run `docker-compose build migration`.

You can run Elasticsearch only with `docker-compose run elasticsearch` if you wish to develop outside of docker.
