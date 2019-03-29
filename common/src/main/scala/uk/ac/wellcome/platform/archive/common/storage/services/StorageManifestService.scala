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
      fileManifest <- createFileManifest(bagLocation)
      tagManifest <- createTagManifest(bagLocation)
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
        .toObjectLocation(bagLocation)
        .toInputStream

      bagInfo <- BagInfoParser.create(
        bagInfoInputStream
      )
    } yield bagInfo

  def createFileManifest(bagLocation: BagLocation): Future[FileManifest] =
    createManifest(
      s"manifest-$checksumAlgorithm.txt",
      bagLocation
    )

  def createTagManifest(bagLocation: BagLocation): Future[FileManifest] =
    createManifest(
      s"tagmanifest-$checksumAlgorithm.txt",
      bagLocation
    )

  private def createManifest(
    name: String,
    bagLocation: BagLocation
  ): Future[FileManifest] =
    for {
      fileManifestInputStream <- BagItemPath(name)
        .toObjectLocation(bagLocation)
        .toInputStream

      fileManifest <- FileManifestParser.create(
        fileManifestInputStream,
        checksumAlgorithm
      )
    } yield fileManifest

}
