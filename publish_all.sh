#!/bin/bash

export AWS_PROFILE=storage

make bag_register-publish
make bag_replicator-publish
make bag_unpacker-publish
make bag_verifier-publish
make bag_versioner-publish

make ingests-publish
make notifier-publish
make replica_aggregator-publish

make bags_api-publish
make ingests_api-publish