package uk.ac.wellcome.platform.archive.common.storage.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  BagIt,
  BagItemPath,
  BagLocation
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
  StorageManifest
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}

class StorageManifestService(
  implicit
  executionContext: ExecutionContext,
  s3Client: AmazonS3
) {

  val checksumAlgorithm = ChecksumAlgorithm("sha256")

  def createManifest(bagLocation: BagLocation): Future[StorageManifest] =
    for {
      bagInfo <- createBagInfo(bagLocation)
      fileManifest <- createFileManifest(bagLocation.objectLocation)
      tagManifest <- createTagManifest(bagLocation.objectLocation)
    } yield
      StorageManifest(
        space = bagLocation.storageSpace,
        info = bagInfo,
        manifest = fileManifest,
        tagManifest = tagManifest,
        locations = List(
          StorageLocation(
            provider = InfrequentAccessStorageProvider,
            location = bagLocation.objectLocation
          )
        ),
        createdDate = Instant.now()
      )

  def createBagInfo(bagLocation: BagLocation): Future[BagInfo] =
    for {
      bagInfoInputStream <- BagIt.bagInfoPath
        .toObjectLocation(bagLocation.objectLocation)
        .toInputStream

      bagInfo <- BagInfoParser.create(
        bagInfoInputStream
      )
    } yield bagInfo

  def createFileManifest(
    bagRootLocation: ObjectLocation): Future[FileManifest] =
    createManifest(
      s"manifest-$checksumAlgorithm.txt",
      bagRootLocation
    )

  def createTagManifest(bagRootLocation: ObjectLocation): Future[FileManifest] =
    createManifest(
      s"tagmanifest-$checksumAlgorithm.txt",
      bagRootLocation
    )

  private def createManifest(
    name: String,
    bagRootLocation: ObjectLocation
  ): Future[FileManifest] = {
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
