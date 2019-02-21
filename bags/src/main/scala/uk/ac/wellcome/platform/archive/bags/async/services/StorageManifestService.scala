package uk.ac.wellcome.platform.archive.bags.async.services

import java.io.InputStream
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.bags.async.models.BagManifestUpdate
import uk.ac.wellcome.platform.archive.bags.common.models.{
  ChecksumAlgorithm,
  FileManifest,
  StorageManifest
}
import uk.ac.wellcome.platform.archive.common.bag.{
  BagDigestFileCreator,
  BagInfoParser
}
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagDigestFile,
  BagInfo,
  BagIt
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

  def createManifest(bagManifestUpdate: BagManifestUpdate)
    : Future[Either[Throwable, StorageManifest]] = {
    val future: Future[StorageManifest] = for {
      bagInfoInputStream <- downloadFile(
        bagManifestUpdate = bagManifestUpdate,
        filename = BagIt.BagInfoFilename
      )
      bagInfo <- Future.fromTry {
        parseBagInfo(
          bagManifestUpdate = bagManifestUpdate,
          bagInfoInputStream = bagInfoInputStream
        )
      }
      manifestTuples <- getBagItems(
        bagManifestUpdate = bagManifestUpdate,
        filename = s"manifest-$algorithm.txt"
      )
      tagManifestTuples <- getBagItems(
        bagManifestUpdate = bagManifestUpdate,
        filename = s"tagmanifest-$algorithm.txt"
      )
    } yield {
      StorageManifest(
        space = bagManifestUpdate.archiveBagLocation.storageSpace,
        info = bagInfo,
        manifest = FileManifest(checksumAlgorithm, manifestTuples),
        tagManifest = FileManifest(checksumAlgorithm, tagManifestTuples),
        accessLocation = StorageLocation(
          provider = InfrequentAccessStorageProvider,
          location = bagManifestUpdate.accessBagLocation.objectLocation
        ),
        archiveLocations = List(
          StorageLocation(
            provider = InfrequentAccessStorageProvider,
            location = bagManifestUpdate.archiveBagLocation.objectLocation
          )
        ),
        createdDate = Instant.now()
      )
    }

    future
      .map { manifest =>
        Right(manifest)
      }
      .recover { case err: Throwable => Left(err) }
  }

  private def parseBagInfo(bagManifestUpdate: BagManifestUpdate,
                           bagInfoInputStream: InputStream): Try[BagInfo] =
    BagInfoParser.parseBagInfo(
      bagManifestUpdate,
      inputStream = bagInfoInputStream) match {
      case Right(bagInfo) => Success(bagInfo)
      case Left(err)      => Failure(throw new RuntimeException(err.toString))
    }

  private def getBagItems(bagManifestUpdate: BagManifestUpdate,
                          filename: String): Future[List[BagDigestFile]] =
    for {
      inputStream <- downloadFile(
        bagManifestUpdate = bagManifestUpdate,
        filename = filename)
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

  private def downloadFile(bagManifestUpdate: BagManifestUpdate,
                           filename: String): Future[InputStream] = {
    val bagLocation = bagManifestUpdate.archiveBagLocation
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
