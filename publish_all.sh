#!/bin/bash

export AWS_PROFILE=storage-dev

make bag_register-publish
make bag_replicator-publish
make bag_unpacker-publish
make bag_verifier-publish
make bagger-publish
make bags_api-publish
make ingests-publish
make ingests_api-publish
make notifier-publish
