package uk.ac.wellcome.platform.storage.replica_aggregator.models

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

// This needs a dynamo format (see dynamo id for unambiguous representation)
case class ReplicaIdentifier(
                              storageSpace: StorageSpace,
                              externalIdentifier: ExternalIdentifier,
                              version: BagVersion
                            )
