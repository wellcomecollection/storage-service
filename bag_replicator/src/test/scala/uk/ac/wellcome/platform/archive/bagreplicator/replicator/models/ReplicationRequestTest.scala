package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagreplicator.models.{PrimaryReplica, SecondaryReplica}
import uk.ac.wellcome.platform.archive.common.ingests.models.{AmazonS3StorageProvider, AzureBlobStorageProvider}
import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryS3ReplicaLocation, SecondaryAzureReplicaLocation, SecondaryS3ReplicaLocation}
import uk.ac.wellcome.storage.{AzureBlobItemLocationPrefix, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures

class ReplicationRequestTest extends AnyFunSpec with Matchers with NewS3Fixtures {
  describe("toReplicaLocation") {
    val dstPrefix = createObjectLocationPrefix

    val request = ReplicationRequest(
      srcPrefix = createS3ObjectLocationPrefix,
      dstPrefix = dstPrefix
    )

    it("a primary S3 replica") {
      request.toReplicaLocation(
        provider = AmazonS3StorageProvider,
        replicaType = PrimaryReplica
      ) shouldBe PrimaryS3ReplicaLocation(prefix = S3ObjectLocationPrefix(dstPrefix))
    }

    it("a secondary S3 replica") {
      request.toReplicaLocation(
        provider = AmazonS3StorageProvider,
        replicaType = SecondaryReplica
      ) shouldBe SecondaryS3ReplicaLocation(prefix = S3ObjectLocationPrefix(dstPrefix))
    }

    it("a secondary Azure replica") {
      request.toReplicaLocation(
        provider = AzureBlobStorageProvider,
        replicaType = SecondaryReplica
      ) shouldBe SecondaryAzureReplicaLocation(prefix = AzureBlobItemLocationPrefix(dstPrefix))
    }

    it("does not allow a primary Azure replica") {
      intercept[IllegalArgumentException] {
        request.toReplicaLocation(
          provider = AzureBlobStorageProvider,
          replicaType = PrimaryReplica
        )
      }
    }
  }
}
