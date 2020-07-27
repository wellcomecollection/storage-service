package uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.ReplicatorTestCases
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.s3.S3Tags

class S3ReplicatorTest
    extends ReplicatorTestCases[
      Bucket,
      S3ObjectLocation,
      S3ObjectLocationPrefix
    ]
    with S3Fixtures {

  override def withDstNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withReplicator[R](testWith: TestWith[ReplicatorImpl, R]): R =
    testWith(new S3Replicator())

  override def createDstLocationWith(
    dstBucket: Bucket,
    key: String
  ): S3ObjectLocation =
    S3ObjectLocation(bucket = dstBucket.name, key = key)

  override def createDstPrefixWith(dstBucket: Bucket): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(dstBucket.name, keyPrefix = "")

  override val dstTags: Tags[S3ObjectLocation] = new S3Tags()

  override val dstStringStore: S3TypedStore[String] = S3TypedStore[String]
}
