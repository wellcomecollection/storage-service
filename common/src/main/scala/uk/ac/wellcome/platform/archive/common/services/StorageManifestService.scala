package uk.ac.wellcome.platform.archive.common.services

import java.io.InputStream
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.models.bagit._
import uk.ac.wellcome.platform.archive.common.models.{ChecksumAlgorithm, FileManifest, StorageManifest}
import uk.ac.wellcome.platform.archive.common.parsers.{BagInfoParser, FileManifestParser}
import uk.ac.wellcome.platform.archive.common.progress.models.{InfrequentAccessStorageProvider, StorageLocation}
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}

class StorageManifestService(
  implicit
    executionContext: ExecutionContext,
    s3Client: AmazonS3
) {

  val checksumAlgorithm = ChecksumAlgorithm("sha256")

  def createManifest(bagLocation: BagLocation): Future[StorageManifest] = for {
    bagInfo <- createBagInfo(bagLocation)
    fileManifest <- createFileManifest(bagLocation)
    tagManifest <- createTagManifest(bagLocation)
  } yield StorageManifest(
    space = bagLocation.storageSpace,
    info = bagInfo,
    manifest = fileManifest,
    tagManifest = tagManifest,
    accessLocation = StorageLocation(
      provider = InfrequentAccessStorageProvider,
      location = bagLocation.objectLocation
    ),
    archiveLocations = List.empty,
    createdDate = Instant.now()
  )

  def createBagInfo(bagLocation: BagLocation): Future[BagInfo] = for {
    bagInfoInputStream <- getInputStream(
      BagIt.bagInfoPath
        .toObjectLocation(bagLocation)
    )

    bagInfo <- BagInfoParser.create(
      bagInfoInputStream
    )
  } yield bagInfo

  def createFileManifest(bagLocation: BagLocation
                        ): Future[FileManifest] = {
    createManifest(
      s"manifest-$checksumAlgorithm.txt",
      bagLocation
    )
  }

  def createTagManifest(bagLocation: BagLocation
                       ): Future[FileManifest] = {
    createManifest(
      s"tagmanifest-$checksumAlgorithm.txt",
      bagLocation
    )
  }

  private def createManifest(
                              name: String,
                              bagLocation: BagLocation
                            ): Future[FileManifest] = for {
    fileManifestInputStream <-
      getInputStream(
        BagItemPath(name)
          .toObjectLocation(bagLocation)
      )

    fileManifest <- FileManifestParser.create(
      fileManifestInputStream, checksumAlgorithm
    )
  } yield fileManifest

  private def getInputStream(objectLocation: ObjectLocation): Future[InputStream] =
    Future {
      s3Client
        .getObject(objectLocation.namespace, objectLocation.key)
        .getObjectContent
    }
}
