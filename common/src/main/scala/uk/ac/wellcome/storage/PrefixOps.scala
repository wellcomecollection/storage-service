package uk.ac.wellcome.storage

import java.nio.file.Paths

object PrefixOps {
  implicit class S3ObjectLocationPrefixOps(location: S3ObjectLocationPrefix) {
    def parent: S3ObjectLocationPrefix =
      location.copy(
        keyPrefix = Paths.get(location.keyPrefix).getParent.toString
      )

    def basename: String =
      Paths.get(location.keyPrefix).getFileName.toString
  }

  implicit class AzureBlobItemLocationPrefixOps(
    location: AzureBlobItemLocationPrefix
  ) {

    def parent: AzureBlobItemLocationPrefix =
      location.copy(
        namePrefix = Paths.get(location.namePrefix).getParent.toString
      )

    def basename: String =
      Paths.get(location.namePrefix).getFileName.toString
  }
}
