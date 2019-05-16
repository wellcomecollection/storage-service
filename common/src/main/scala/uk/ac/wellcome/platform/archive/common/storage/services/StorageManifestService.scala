package uk.ac.wellcome.platform.archive.common.storage.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  BagItemPath
}
import uk.ac.wellcome.platform.archive.common.bagit.parsers.{
  BagInfoParser,
  FileManifestParser
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  ChecksumAlgorithm,
  FileManifest,
  StorageManifest,
  StorageSpace
}
import uk.ac.wellcome.storage.ObjectLocation
import S3StreamableInstances._

import scala.util.Try

class StorageManifestService(implicit s3Client: AmazonS3) {

  val checksumAlgorithm = ChecksumAlgorithm("sha256")

  def createManifest(
    bagRootLocation: ObjectLocation,
    storageSpace: StorageSpace
  ): Try[StorageManifest] =
    for {
      bagInfo <- createBagInfo(bagRootLocation)
      fileManifest <- createFileManifest(bagRootLocation)
      tagManifest <- createTagManifest(bagRootLocation)
    } yield
      StorageManifest(
        space = storageSpace,
        info = bagInfo,
        manifest = fileManifest,
        tagManifest = tagManifest,
        locations = List(
          StorageLocation(
            provider = InfrequentAccessStorageProvider,
            location = bagRootLocation
          )
        ),
        createdDate = Instant.now()
      )

  def createBagInfo(bagRootLocation: ObjectLocation): Try[BagInfo] =
    for {
      bagInfoInputStream <- BagItemPath("bag-info.txt")
        .toObjectLocation(bagRootLocation)
        .toInputStream

      bagInfo <- BagInfoParser.create(bagInfoInputStream)
    } yield bagInfo

  def createFileManifest(bagRootLocation: ObjectLocation): Try[FileManifest] =
    createManifest(
      s"manifest-$checksumAlgorithm.txt",
      bagRootLocation
    )

  def createTagManifest(bagRootLocation: ObjectLocation): Try[FileManifest] =
    createManifest(
      s"tagmanifest-$checksumAlgorithm.txt",
      bagRootLocation
    )

  private def createManifest(
    name: String,
    bagRootLocation: ObjectLocation
  ): Try[FileManifest] = {
    for {
      fileManifestInputStream <- BagItemPath(name)
        .toObjectLocation(bagRootLocation)
        .toInputStream

      fileManifest <- FileManifestParser.create(
        fileManifestInputStream,
        checksumAlgorithm
      )
    } yield fileManifest
  }

}
