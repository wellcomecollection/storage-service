package uk.ac.wellcome.platform.archive.common.fixtures.s3

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.s3.S3TypedStore

trait S3BagBuilder
    extends BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket]
    with S3Fixtures {

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

  def createS3BagWith(
    bucket: Bucket,
    space: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    payloadFileCount: Int = randomInt(from = 5, to = 50)
  ): (S3ObjectLocationPrefix, BagInfo) = {
    implicit val namespace: Bucket = bucket

    val bagContents = createBagContentsWith(
      space = space,
      externalIdentifier = externalIdentifier,
      payloadFileCount = payloadFileCount
    )

    implicit val typedStore: S3TypedStore[String] = S3TypedStore[String]
    uploadBagObjects(bagContents.bagRoot, objects = bagContents.bagObjects)

    (bagContents.bagRoot, bagContents.bagInfo)
  }
}
