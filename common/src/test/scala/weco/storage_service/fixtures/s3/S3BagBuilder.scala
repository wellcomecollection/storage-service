package weco.storage_service.fixtures.s3

import weco.storage_service.bagit.models.{
  BagInfo,
  BagVersion,
  ExternalIdentifier
}
import weco.storage_service.fixtures.BagBuilder
import weco.storage_service.storage.models.StorageSpace
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import weco.storage.store.TypedStore
import weco.storage.store.s3.S3TypedStore

trait S3BagBuilder
    extends BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket]
    with S3Fixtures {
  implicit val typedStore: TypedStore[S3ObjectLocation, String] =
    S3TypedStore[String]

  override def createBagRoot(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  )(
    bucket: Bucket
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

  def storeS3BagWith(
    space: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: BagVersion = BagVersion(randomInt(from = 2, to = 10)),
    payloadFileCount: Int = randomInt(from = 5, to = 50)
  )(implicit bucket: Bucket): (S3ObjectLocationPrefix, BagInfo) =
    storeBagWith(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version,
      payloadFileCount = payloadFileCount
    )(namespace = bucket, primaryBucket = bucket)
}
