package weco.storage.transfer.s3

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.scalatestplus.mockito.MockitoSugar
import weco.fixtures.TestWith
import weco.storage.ListingFailure
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.generators.{Record, RecordGenerators}
import weco.storage.listing.s3.{S3ObjectLocationListing, S3ObjectListing}
import weco.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.s3.{S3TypedStore, S3TypedStoreFixtures}
import weco.storage.transfer._

class S3PrefixTransferTest
    extends PrefixTransferTestCases[
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      S3ObjectLocation,
      S3ObjectLocationPrefix,
      Record,
      Bucket,
      Bucket,
      S3TypedStore[Record],
      S3TypedStore[Record],
      Unit]
    with RecordGenerators
    with S3TypedStoreFixtures[Record]
    with S3TransferFixtures[Record]
    with MockitoSugar {

  def createSrcPrefix(srcBucket: Bucket): S3ObjectLocationPrefix =
    createS3ObjectLocationPrefixWith(srcBucket)

  def createDstPrefix(dstBucket: Bucket): S3ObjectLocationPrefix =
    createS3ObjectLocationPrefixWith(dstBucket)

  def createSrcLocationFrom(srcPrefix: S3ObjectLocationPrefix,
                            suffix: String): S3ObjectLocation =
    srcPrefix.asLocation(suffix)

  def createDstLocationFrom(dstPrefix: S3ObjectLocationPrefix,
                            suffix: String): S3ObjectLocation =
    dstPrefix.asLocation(suffix)

  def withPrefixTransfer[R](srcStore: S3TypedStore[Record],
                            dstStore: S3TypedStore[Record])(
    testWith: TestWith[PrefixTransferImpl, R]
  ): R =
    testWith(S3PrefixTransfer())

  def withExtraListingTransfer[R](
    srcStore: S3TypedStore[Record],
    dstStore: S3TypedStore[Record]
  )(
    testWith: TestWith[PrefixTransferImpl, R]
  ): R = {
    implicit val summaryListing: S3ObjectListing =
      new S3ObjectListing()
    implicit val listing: S3ObjectLocationListing =
      new S3ObjectLocationListing() {
        override def list(prefix: S3ObjectLocationPrefix): ListingResult =
          super.list(prefix).map { _ ++ Seq(createS3ObjectLocation) }
      }

    implicit val transfer: S3Transfer = S3Transfer.apply

    testWith(new S3PrefixTransfer())
  }

  def withBrokenListingTransfer[R](
    srcStore: S3TypedStore[Record],
    dstStore: S3TypedStore[Record]
  )(
    testWith: TestWith[PrefixTransferImpl, R]
  ): R = {
    implicit val summaryListing: S3ObjectListing =
      new S3ObjectListing()
    implicit val listing: S3ObjectLocationListing =
      new S3ObjectLocationListing() {
        override def list(prefix: S3ObjectLocationPrefix): ListingResult =
          Left(ListingFailure(prefix, e = new Throwable("BOOM!")))
      }

    implicit val transfer: S3Transfer = S3Transfer.apply

    testWith(new S3PrefixTransfer())
  }

  def withBrokenTransfer[R](
    srcStore: S3TypedStore[Record],
    dstStore: S3TypedStore[Record]
  )(
    testWith: TestWith[PrefixTransferImpl, R]
  ): R = {
    implicit val listing: S3ObjectLocationListing = S3ObjectLocationListing()

    implicit val transfer: S3Transfer = mock[S3Transfer]
    when(
      transfer
        .transfer(any[S3ObjectLocation], any[S3ObjectLocation]))
      .thenAnswer((invocation: InvocationOnMock) => {
        val src = invocation.getArgument[S3ObjectLocation](0)
        val dst = invocation.getArgument[S3ObjectLocation](1)
        Left(TransferSourceFailure(src, dst))
      })

    testWith(new S3PrefixTransfer())
  }

  def createT: Record = createRecord

  def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())
}
