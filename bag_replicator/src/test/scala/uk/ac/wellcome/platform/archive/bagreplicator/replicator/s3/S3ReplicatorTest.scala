package uk.ac.wellcome.platform.archive.bagreplicator.replicator.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.{
  Replicator,
  ReplicatorTestCases
}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.s3.S3Tags
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

class S3ReplicatorTest
    extends ReplicatorTestCases[Bucket]
    with S3Fixtures {

  override def withDstNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withReplicator[R](testWith: TestWith[Replicator, R]): R =
    testWith(new S3Replicator())

  override def createDstLocationWith(
    dstBucket: Bucket,
    key: String
  ): ObjectLocation =
    createObjectLocationWith(dstBucket, key)

  override def createDstPrefixWith(dstBucket: Bucket): ObjectLocationPrefix =
    ObjectLocationPrefix(dstBucket.name, path = "")

  override val dstTags: Tags[ObjectLocation] = new S3Tags()

  override val dstStringStore: S3TypedStore[String] = S3TypedStore[String]
}
