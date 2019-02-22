package uk.ac.wellcome.platform.archive.common.services

import java.io.InputStream
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bag.{
  BagDigestFileCreator,
  BagInfoParser
}
import uk.ac.wellcome.platform.archive.common.models.{
  BagRequest,
  ChecksumAlgorithm,
  FileManifest,
  StorageManifest
}
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagDigestFile,
  BagInfo,
  BagIt,
  BagLocation
}
import uk.ac.wellcome.platform.archive.common.progress.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class StorageManifestService(s3Client: AmazonS3)(
  implicit ec: ExecutionContext) {

  val algorithm = "sha256"
  val checksumAlgorithm = ChecksumAlgorithm(algorithm)

  def createManifest(bagRequest: BagRequest): Future[StorageManifest] =
    for {
      bagInfoInputStream <- downloadFile(
        bagLocation = bagRequest.bagLocation,
        filename = BagIt.BagInfoFilename
      )
      bagInfo <- Future.fromTry {
        parseBagInfo(bagInfoInputStream = bagInfoInputStream)
      }
      manifestTuples <- getBagItems(
        bagLocation = bagRequest.bagLocation,
        filename = s"manifest-$algorithm.txt"
      )
      tagManifestTuples <- getBagItems(
        bagLocation = bagRequest.bagLocation,
        filename = s"tagmanifest-$algorithm.txt"
      )
    } yield {
      StorageManifest(
        space = bagRequest.bagLocation.storageSpace,
        info = bagInfo,
        manifest = FileManifest(checksumAlgorithm, manifestTuples),
        tagManifest = FileManifest(checksumAlgorithm, tagManifestTuples),
        accessLocation = StorageLocation(
          provider = InfrequentAccessStorageProvider,
          location = bagRequest.bagLocation.objectLocation
        ),
        archiveLocations = List.empty,
        createdDate = Instant.now()
      )
    }

  private def parseBagInfo(bagInfoInputStream: InputStream): Try[BagInfo] =
    BagInfoParser.parseBagInfo(
      (),
      inputStream = bagInfoInputStream) match {
      case Right(bagInfo) => Success(bagInfo)
      case Left(err)      => Failure(throw new RuntimeException(err.toString))
    }

  private def getBagItems(bagLocation: BagLocation,
                          filename: String): Future[List[BagDigestFile]] =
    for {
      inputStream <- downloadFile(
        bagLocation = bagLocation,
        filename = filename
      )
      lines: List[String] = scala.io.Source
        .fromInputStream(inputStream)
        .mkString
        .split("\n")
        .filter { _.nonEmpty }
        .toList
      tryBagDigestFiles: List[Try[BagDigestFile]] = lines.map { line =>
        BagDigestFileCreator.create(
          line = line,
          bagRootPathInZip = None,
          manifestName = filename
        )
      }
      bagDigestFiles: List[BagDigestFile] <- Future.sequence {
        tryBagDigestFiles.map { Future.fromTry }
      }
    } yield bagDigestFiles

  private def downloadFile(bagLocation: BagLocation,
                           filename: String): Future[InputStream] = {
    val location = ObjectLocation(
      namespace = bagLocation.storageNamespace,
      key = List(bagLocation.completePath, filename).mkString("/")
    )

    Future {
      s3Client
        .getObject(location.namespace, location.key)
        .getObjectContent
    }
  }
}
