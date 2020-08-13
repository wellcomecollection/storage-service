package uk.ac.wellcome.platform.archive.common.fixtures.s3

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.s3.S3TypedStore

trait S3BagBuilder
    extends BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket]
    with S3Fixtures {
  implicit val typedStore: TypedStore[S3ObjectLocation,String] = S3TypedStore[String]

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
}
