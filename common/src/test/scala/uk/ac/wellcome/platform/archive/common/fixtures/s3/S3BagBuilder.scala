package uk.ac.wellcome.platform.archive.common.fixtures.s3

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.DestinationBuilder
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

trait S3BagBuilder
  extends BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix]
    with NewS3Fixtures {

  override def createBagRoot(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  )(
    implicit namespace: String
  ): S3ObjectLocationPrefix =
    S3ObjectLocationPrefix(
      bucket = namespace,
      keyPrefix = DestinationBuilder.buildPath(space, externalIdentifier, version)
    )

  def createBagLocation(bagRoot: S3ObjectLocationPrefix, path: String): S3ObjectLocation =
    S3ObjectLocation(
      bucket = bagRoot.bucket,
      key = path
    )
}
