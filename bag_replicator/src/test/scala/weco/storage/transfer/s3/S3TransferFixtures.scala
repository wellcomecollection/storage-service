package weco.storage.transfer.s3

import weco.fixtures.TestWith
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.s3.S3ObjectLocation
import weco.storage.store.s3.{S3TypedStore, S3TypedStoreFixtures}
import weco.storage.transfer.Transfer
import weco.storage.transfer.fixtures.TransferFixtures

trait S3TransferFixtures[T]
    extends TransferFixtures[S3ObjectLocation, T, S3TypedStore[T]]
    with S3TypedStoreFixtures[T] {
  override def withTransferStore[R](initialEntries: Map[S3ObjectLocation, T])(
    testWith: TestWith[S3TypedStore[T], R]): R =
    withTypedStoreImpl(storeContext = (), initialEntries = initialEntries) {
      typedStore =>
        testWith(typedStore)
    }

  override def withTransfer[R](
    testWith: TestWith[Transfer[S3ObjectLocation, S3ObjectLocation], R])(
    implicit store: S3TypedStore[T]): R =
    testWith(S3Transfer.apply)

  def withSrcNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  def withDstNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  def createSrcLocation(srcBucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(srcBucket)

  def createDstLocation(dstBucket: Bucket): S3ObjectLocation =
    createS3ObjectLocationWith(dstBucket)

  def withSrcStore[R](initialEntries: Map[S3ObjectLocation, T])(
    testWith: TestWith[S3TypedStore[T], R])(implicit context: Unit): R =
    withTypedStoreImpl(context, initialEntries = initialEntries) { typedStore =>
      testWith(typedStore)
    }

  def withDstStore[R](initialEntries: Map[S3ObjectLocation, T])(
    testWith: TestWith[S3TypedStore[T], R])(implicit context: Unit): R =
    withTypedStoreImpl(context, initialEntries = initialEntries) { typedStore =>
      testWith(typedStore)
    }
}
