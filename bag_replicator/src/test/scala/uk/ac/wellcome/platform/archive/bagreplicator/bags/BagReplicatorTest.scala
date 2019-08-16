package uk.ac.wellcome.platform.archive.bagreplicator.bags

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagreplicator.bags.models.{BagReplicationSucceeded, PrimaryBagReplicationRequest}
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.ReplicationRequest
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3.S3Replicator
import uk.ac.wellcome.platform.archive.common.fixtures.S3BagBuilder
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.store.s3.S3StreamStore

import scala.concurrent.ExecutionContext.Implicits.global

class BagReplicatorTest extends FunSpec with Matchers with S3Fixtures with BagReplicatorFixtures with ScalaFutures with IntegrationPatience {
  val replicator: S3Replicator = new S3Replicator()

  implicit val streamStore: S3StreamStore = new S3StreamStore()

  it("replicates a bag successfully") {
    val bagReplicator = new BagReplicator[PrimaryBagReplicationRequest](replicator)

    withLocalS3Bucket { bucket =>
      val (bagRoot, _) = S3BagBuilder.createS3BagWith(bucket)

      val srcPrefix = bagRoot.asPrefix

      val dstPrefix = ObjectLocationPrefix(
        namespace = bucket.name,
        path = "dst/"
      )

      val request = PrimaryBagReplicationRequest(
        ReplicationRequest(
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix
        )
      )

      val future = bagReplicator.replicateBag(request)

      whenReady(future) { result =>
        result shouldBe a[BagReplicationSucceeded[_]]
        result.summary.request shouldBe request

        verifyObjectsCopied(
          src = srcPrefix.asLocation(),
          dst = dstPrefix.asLocation()
        )
      }
    }
  }

  it("wraps an error from the underlying replicator") {
    true shouldBe false
  }

  it("catches an exception from the underlying replicator") {
    true shouldBe false
  }

  describe("checks the tag manifests match") {
    it("errors if there is no tag manifest") {
      true shouldBe false
    }

    it("errors if the tag manifests do not match") {
      true shouldBe false
    }
  }

}
