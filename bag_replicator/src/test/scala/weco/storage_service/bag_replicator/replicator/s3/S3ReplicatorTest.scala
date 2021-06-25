package weco.storage_service.bag_replicator.replicator.s3

import weco.fixtures.TestWith
import weco.storage_service.bag_replicator.replicator.ReplicatorTestCases
import weco.storage_service.bag_replicator.replicator.models.{
  ReplicationRequest,
  ReplicationSucceeded
}
import weco.storage.Identified
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.listing.Listing
import weco.storage.listing.s3.S3ObjectLocationListing
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.s3.S3TypedStore
import weco.storage.transfer.s3.S3PrefixTransfer

class S3ReplicatorTest
    extends ReplicatorTestCases[
      Bucket,
      S3ObjectLocation,
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      S3PrefixTransfer
    ]
    with S3Fixtures {

  override def withDstNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def createDstLocationWith(
    dstBucket: Bucket,
    key: String
  ): S3ObjectLocation =
    S3ObjectLocation(bucket = dstBucket.name, key = key)

  override def createDstPrefixWith(dstBucket: Bucket): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(dstBucket.name, keyPrefix = "")

  override val dstStringStore: S3TypedStore[String] = S3TypedStore[String]

  override def withPrefixTransfer[R](
    testWith: TestWith[S3PrefixTransfer, R]
  ): R =
    testWith(S3PrefixTransfer())

  override def withReplicator[R](
    prefixTransferImpl: S3PrefixTransfer
  )(testWith: TestWith[ReplicatorImpl, R]): R =
    testWith(new S3Replicator() {
      override val prefixTransfer: S3PrefixTransfer = prefixTransferImpl
    })

  override val dstListing: Listing[S3ObjectLocationPrefix, S3ObjectLocation] =
    S3ObjectLocationListing()

  // The verifier will write a Content-SHA256 checksum tag to objects when it
  // verifies them.  If an object is then replicated to a new location, any existing
  // verification tags should be removed.
  it("doesn't copy tags from the existing objects") {
    withSrcNamespace { srcNamespace =>
      withDstNamespace { dstNamespace =>
        val location = createSrcLocationWith(srcNamespace)

        putSrcObject(location, contents = randomAlphanumeric())
        srcTags.update(location) { existingTags =>
          Right(existingTags ++ Map("Content-SHA256" -> "abcdef"))
        }

        val request = ReplicationRequest(
          srcPrefix = createSrcPrefixWith(srcNamespace),
          dstPrefix = createDstPrefixWith(dstNamespace)
        )

        val result = withReplicator {
          _.replicate(
            ingestId = createIngestID,
            request = request
          )
        }

        result shouldBe a[ReplicationSucceeded[_]]

        val dstLocation = createDstLocationWith(
          dstNamespace,
          key = location.key
        )

        srcTags.get(dstLocation).value shouldBe Identified(
          dstLocation,
          Map.empty
        )
      }
    }
  }
}
