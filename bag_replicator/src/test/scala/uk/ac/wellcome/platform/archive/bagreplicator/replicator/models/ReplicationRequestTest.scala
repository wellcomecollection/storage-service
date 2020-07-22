package uk.ac.wellcome.platform.archive.bagreplicator.replicator.models

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagreplicator.models.{
  PrimaryReplica,
  SecondaryReplica
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryS3ReplicaLocation,
  SecondaryAzureReplicaLocation,
  SecondaryS3ReplicaLocation
}
import uk.ac.wellcome.storage.AzureBlobItemLocationPrefix
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures

class ReplicationRequestTest
    extends AnyFunSpec
    with Matchers
    with NewS3Fixtures {
  describe("toReplicaLocation") {
    val s3Prefix = createS3ObjectLocationPrefix
    val azurePrefix = AzureBlobItemLocationPrefix(
      container = randomAlphanumeric,
      namePrefix = randomAlphanumeric
    )

    val s3Request = ReplicationRequest(
      srcPrefix = createS3ObjectLocationPrefix,
      dstPrefix = s3Prefix
    )

    val azureRequest = ReplicationRequest(
      srcPrefix = createS3ObjectLocationPrefix,
      dstPrefix = azurePrefix
    )

    it("a primary S3 replica") {
      s3Request.toReplicaLocation(replicaType = PrimaryReplica) shouldBe PrimaryS3ReplicaLocation(
        prefix = s3Prefix
      )
    }

    it("a secondary S3 replica") {
      s3Request.toReplicaLocation(replicaType = SecondaryReplica) shouldBe SecondaryS3ReplicaLocation(
        prefix = s3Prefix
      )
    }

    it("a secondary Azure replica") {
      azureRequest.toReplicaLocation(replicaType = SecondaryReplica) shouldBe SecondaryAzureReplicaLocation(
        prefix = azurePrefix
      )
    }

    it("does not allow a primary Azure replica") {
      intercept[IllegalArgumentException] {
        azureRequest.toReplicaLocation(replicaType = PrimaryReplica)
      }
    }
  }
}
