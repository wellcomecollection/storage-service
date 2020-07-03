package uk.ac.wellcome.platform.archive.common.fixtures.s3

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.s3.NewS3TypedStore
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

import scala.util.Random

trait S3BagBuilder
    extends BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket]
    with NewS3Fixtures {

  override def createBagRoot(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  )(
    implicit bucket: Bucket
  ): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = bucket.name,
      keyPrefix = createBagRootPath(space, externalIdentifier, version)
    )

  def createBagLocation(
    bagRoot: S3ObjectLocationPrefix,
    path: String
  ): S3ObjectLocation =
    S3ObjectLocation(
      bucket = bagRoot.bucket,
      key = path
    )

  override protected def buildFetchEntryLine(
    entry: PayloadEntry
  )(implicit bucket: Bucket): String = {
    val displaySize =
      if (Random.nextBoolean()) entry.contents.getBytes.length.toString else "-"

    s"""s3://${bucket.name}/${entry.path} $displaySize ${entry.bagPath}"""
  }

  def createS3BagWith(
    bucket: Bucket,
    space: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    payloadFileCount: Int = randomInt(from = 5, to = 50)
  ): (S3ObjectLocationPrefix, BagInfo) = {
    implicit val namespace: Bucket = bucket

    val (bagObjects, bagRoot, bagInfo) = createBagContentsWith(
      space = space,
      externalIdentifier = externalIdentifier,
      payloadFileCount = payloadFileCount
    )

    implicit val typedStore: NewS3TypedStore[String] = NewS3TypedStore[String]
    uploadBagObjects(bagObjects)

    (bagRoot, bagInfo)
  }
}
