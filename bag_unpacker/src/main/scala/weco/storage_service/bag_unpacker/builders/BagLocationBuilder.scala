package weco.storage_service.bag_unpacker.builders

import java.nio.file.Paths

import weco.storage_service.bag_unpacker.config.models.BagUnpackerWorkerConfig
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models.StorageSpace
import weco.storage.s3.S3ObjectLocationPrefix

object BagLocationBuilder {
  def build(
    ingestId: IngestID,
    storageSpace: StorageSpace,
    unpackerWorkerConfig: BagUnpackerWorkerConfig
  ): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = unpackerWorkerConfig.dstNamespace,
      keyPrefix = Paths
        .get(
          storageSpace.toString,
          ingestId.toString
        )
        .toString
    )
}
