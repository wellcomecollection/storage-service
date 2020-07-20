package uk.ac.wellcome.platform.archive.bagreplicator.bags

import java.time.Instant

import org.scalatest.{Assertion, TryValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagreplicator.bags.models.{
  BagReplicationFailed,
  BagReplicationSucceeded,
  PrimaryBagReplicationRequest
}
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.models.{
  ReplicationFailed,
  ReplicationRequest,
  ReplicationResult,
  ReplicationSummary
}
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3.S3Replicator
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.storage.ObjectLocationPrefix

class BagReplicatorTest
    extends AnyFunSpec
    with Matchers
    with BagReplicatorFixtures
    with S3BagBuilder
    with TryValues {
  val replicator: S3Replicator = new S3Replicator()

  it("replicates a bag successfully") {
    val bagReplicator =
      new BagReplicator(replicator)

    withLocalS3Bucket { bucket =>
      val (srcPrefix, _) = createS3BagWith(bucket)

      val dstPrefix = ObjectLocationPrefix(
        namespace = bucket.name,
        path = "dst/"
      )

      val request = PrimaryBagReplicationRequest(
        ReplicationRequest(
          srcPrefix = srcPrefix.toObjectLocationPrefix,
          dstPrefix = dstPrefix
        )
      )

      val result = bagReplicator
        .replicateBag(
          ingestId = createIngestID,
          bagRequest = request
        )
        .success
        .value

      result shouldBe a[BagReplicationSucceeded[_]]
      result.summary.request shouldBe request

      verifyObjectsCopied(
        srcPrefix = srcPrefix,
        dstPrefix = dstPrefix
      )
    }
  }

  it("wraps an error from the underlying replicator") {
    val underlyingErr = new Throwable("BOOM!")

    val badReplicator: S3Replicator = new S3Replicator() {
      override def replicate(
        ingestId: IngestID,
        request: ReplicationRequest
      ): ReplicationResult =
        ReplicationFailed(
          ReplicationSummary(
            ingestId = ingestId,
            startTime = Instant.now,
            maybeEndTime = Option(Instant.now),
            request = request
          ),
          e = underlyingErr
        )
    }

    assertIsFailure(
      bagReplicator = new BagReplicator(badReplicator)
    ) {
      _.e shouldBe underlyingErr
    }
  }

  def assertIsFailure(
    bagReplicator: BagReplicator = new BagReplicator(replicator),
    srcPrefix: ObjectLocationPrefix = createObjectLocationPrefix,
    dstPrefix: ObjectLocationPrefix = createObjectLocationPrefix
  )(assert: BagReplicationFailed[_] => Assertion): Assertion = {
    val request = PrimaryBagReplicationRequest(
      ReplicationRequest(
        srcPrefix = srcPrefix,
        dstPrefix = dstPrefix
      )
    )

    val result = bagReplicator
      .replicateBag(ingestId = createIngestID, bagRequest = request)
      .success
      .value

    result shouldBe a[BagReplicationFailed[_]]
    result.summary.request shouldBe request

    val failure = result.asInstanceOf[BagReplicationFailed[_]]
    assert(failure)
  }
}
