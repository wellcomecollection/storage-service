package uk.ac.wellcome.platform.archive.common.services

import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.parsers.{BagInfoParser, FileManifestParser}
import uk.ac.wellcome.platform.archive.common.models.bagit._
import uk.ac.wellcome.platform.archive.common.models.{BagRequest, ChecksumAlgorithm, StorageManifest}
import uk.ac.wellcome.platform.archive.common.progress.models.{InfrequentAccessStorageProvider, StorageLocation}

import scala.concurrent.{ExecutionContext, Future}

class StorageManifestService(s3Client: AmazonS3)(
  implicit ec: ExecutionContext) {

  val checksumAlgorithm = ChecksumAlgorithm("sha256")

  def createManifest(bagRequest: BagRequest): Future[StorageManifest] =
    for {
      // BagInfo
      bagInfoInputStream <- BagIt
        .bagInfoPath
        .toObjectLocation(
          bagRequest.bagLocation
        )

      bagInfo <- BagInfoParser.create(
        bagInfoInputStream
      )

      // FileManifest
      fileManifestInputStream <- BagItemPath(
        s"manifest-$checksumAlgorithm.txt"
      ).toObjectLocation(bagRequest.bagLocation)

      fileManifest <- FileManifestParser.create(
        fileManifestInputStream, checksumAlgorithm
      )

      // TagManifest
      tagManifestInputStream <- BagItemPath(
        s"tagmanifest-$checksumAlgorithm.txt"
      ).toObjectLocation(bagRequest.bagLocation)

      tagManifest <- FileManifestParser.create(
        tagManifestInputStream, checksumAlgorithm
      )

    } yield {
      StorageManifest(
        space = bagRequest.bagLocation.storageSpace,
        info = bagInfo,
        manifest = fileManifest,
        tagManifest = tagManifest,
        accessLocation = StorageLocation(
          provider = InfrequentAccessStorageProvider,
          location = bagRequest.bagLocation.objectLocation
        ),
        archiveLocations = List.empty,
        createdDate = Instant.now()
      )
    }
}
