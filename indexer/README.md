# indexer

We want to use the storage service to do bulk analysis of our files.
For example, we might ask questions like:

*   How many images did we digitise last month?
*   What file formats are we holding?
*   How big are the files we're storing?

The indexer apps record manifests, files and ingests in Elasticsearch.



## Setting up the indexer users in Elasticsearch

1.  Log in to Kibana in the reporting cluster.
2.  In the left-hand sidebar, select "Management" (the gear icon).
3.  Under "Security", select "Roles".
4.  Click "Create role", and create a new role:

    -   Role name: typically prefixed with `storage_`, e.g. `storage_ingests_write`
    -   Index privileges, indices: an index pattern to match on, e.g. `storage_ingests*`
    -   Index privileges, privileges: `create`, `create_index`, `write`

5.  Under "Security", select "Users".
    Click "Create user", and create a new user:

    -   Username: typically prefixed with `storage_`, e.g. `storage_ingests_indexer`
    -   Password: use your favourite technique of choice
    -   Roles: add the role you created in step 4

6.  Put the following variables in Secrets Manager:

    -   `${label}/ingests_indexer/es_host`
    -   `${label}/ingests_indexer/es_port`
    -   `${label}/ingests_indexer/es_protocol`
    -   `${label}/ingests_indexer/es_username`
    -   `${label}/ingests_indexer/es_password`

    You can use this script to add secrets to Secrets Manager: <https://github.com/wellcomecollection/platform-infrastructure/blob/master/scripts/add_kms_secret.py>
