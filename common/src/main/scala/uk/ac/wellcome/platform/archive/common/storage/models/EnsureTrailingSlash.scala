package uk.ac.wellcome.platform.archive.common.storage.models

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.azure.AzureBlobLocationPrefix
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix
import uk.ac.wellcome.storage.{Location, Prefix}

trait EnsureTrailingSlash[BagPrefix <: Prefix[_ <: Location]] {
  def withTrailingSlash(prefix: BagPrefix): BagPrefix
}

object EnsureTrailingSlash extends Logging {
  private def ensureTrailingSlash(path: String): String =
    if (path.endsWith("/")) {
      debug(s"Path $path ends with a trailing slash; not adding another slash")
      path
    } else if (path == "") {
      debug(s"Path $path is empty; not adding a slash")
      path
    } else {
      debug(s"Path $path is missing a trailing slash; adding a slash")
      s"$path/"
    }

  implicit class EnsureTrailingSlashOps[BagPrefix <: Prefix[_ <: Location]](
    prefix: BagPrefix
  )(
    implicit impl: EnsureTrailingSlash[BagPrefix]
  ) {
    def withTrailingSlash: BagPrefix =
      impl.withTrailingSlash(prefix)
  }

  implicit val s3PrefixTrailingSlash
    : EnsureTrailingSlash[S3ObjectLocationPrefix] =
    (prefix: S3ObjectLocationPrefix) =>
      prefix.copy(keyPrefix = ensureTrailingSlash(prefix.keyPrefix))

  implicit val azurePrefixTrailingSlash
    : EnsureTrailingSlash[AzureBlobLocationPrefix] =
    (prefix: AzureBlobLocationPrefix) =>
      prefix.copy(namePrefix = ensureTrailingSlash(prefix.namePrefix))
}
