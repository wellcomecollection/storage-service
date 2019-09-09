package uk.ac.wellcome.platform.archive.bagreplicator.bags

import java.time.Instant

import org.scalatest.{Assertion, FunSpec, Matchers, TryValues}
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
import uk.ac.wellcome.platform.archive.common.fixtures.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.transfer.s3.{S3PrefixTransfer, S3Transfer}
import uk.ac.wellcome.storage.transfer.{
  TransferFailure,
  TransferPerformed,
  TransferSuccess
}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

class BagReplicatorTest
    extends FunSpec
    with Matchers
    with S3Fixtures
    with BagReplicatorFixtures
    with TryValues {
  val replicator: S3Replicator = new S3Replicator()

  implicit val streamStore: S3StreamStore = new S3StreamStore()

  it("replicates a bag successfully") {
    val bagReplicator =
      new BagReplicator(replicator)

    withLocalS3Bucket { bucket =>
      val (bagRoot, _) = S3BagBuilder.createS3BagWith(bucket)

      val srcPrefix = bagRoot

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

  describe("checks the tag manifests match") {
    it("errors if there is no tag manifest") {
      withLocalS3Bucket { bucket =>
        val (bagRoot, _) = S3BagBuilder.createS3BagWith(bucket)

        s3Client.deleteObject(
          bagRoot.namespace,
          bagRoot.asLocation("tagmanifest-sha256.txt").path
        )

        assertIsFailure(
          srcPrefix = bagRoot,
          dstPrefix = createObjectLocationPrefixWith(bucket.name)
        ) { err =>
          err.e.getMessage should startWith(
            "Unable to load tagmanifest-sha256.txt in source and replica to compare"
          )
        }
      }
    }

    it("errors if the tag manifests do not match") {
      // Create an instance of the bag replicator that writes nonsense
      // to the tag manifest in the replica bag, so the file in the
      // original and the replica.

      implicit val badTransfer: S3Transfer = new S3Transfer() {
        override def transfer(
          src: ObjectLocation,
          dst: ObjectLocation
        ): Either[TransferFailure, TransferSuccess] =
          if (dst.path.endsWith("/tagmanifest-sha256.txt")) {
            s3Client.putObject(
              dst.namespace,
              dst.path,
              "not the tag manifest contents"
            )
            Right(TransferPerformed(src, dst))
          } else {
            super.transfer(src, dst)
          }
      }

      implicit val listing: S3ObjectLocationListing =
        S3ObjectLocationListing()

      val badPrefixTransfer = new S3PrefixTransfer()

      val badReplicator: S3Replicator = new S3Replicator() {
        override val prefixTransfer: S3PrefixTransfer =
          badPrefixTransfer
      }

      withLocalS3Bucket { bucket =>
        val (bagRoot, _) = S3BagBuilder.createS3BagWith(bucket)

        s3Client.deleteObject(
          bagRoot.namespace,
          bagRoot.asLocation("tagmanifest-sha256.txt").path
        )

        assertIsFailure(
          bagReplicator = new BagReplicator(badReplicator),
          srcPrefix = bagRoot,
          dstPrefix = createObjectLocationPrefixWith(bucket.name)
        ) { err =>
          err.e.getMessage should startWith(
            "Unable to load tagmanifest-sha256.txt in source and replica to compare"
          )
        }
      }
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
