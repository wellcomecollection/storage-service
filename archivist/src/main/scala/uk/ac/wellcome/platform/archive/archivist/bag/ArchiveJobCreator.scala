package uk.ac.wellcome.platform.archive.archivist.bag
import java.util.zip.ZipFile

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.archivist.models._
import uk.ac.wellcome.platform.archive.archivist.models.errors.{
  BagNotFoundError,
  FileNotFoundError
}
import uk.ac.wellcome.platform.archive.archivist.zipfile.ZipFileReader
import uk.ac.wellcome.platform.archive.common.parsers.BagInfoParser
import uk.ac.wellcome.platform.archive.common.models._
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagItemPath,
  BagLocation,
  BagPath,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.models.error.ArchiveError

import scala.util.{Failure, Success}

/** This flow extracts the external identifier from the metadata inside
  * the ZIP file, and emits an ArchiveJob.
  *
  * If it's unable to find the identifier, it emits a Left[ArchiveError] instead.
  *
  */
object ArchiveJobCreator extends Logging {

  def create(
    zipFile: ZipFile,
    config: BagUploaderConfig,
    ingestBagRequest: IngestBagRequest
  ): Either[ArchiveError[IngestBagRequest], ArchiveJob] = {
    ZippedBagFile.locateBagInfo(zipFile) match {
      case Success(bagInfoPath) =>
        getBagIdentifier(
          ZipEntryPointer(zipFile, bagInfoPath),
          ingestBagRequest)
          .map { externalIdentifier =>
            val bagRootPathInZip =
              ZippedBagFile.bagPathFromBagInfoPath(bagInfoPath)
            val tagManifestLocation = BagItemPath(
              config.bagItConfig.tagManifestFileName,
              bagRootPathInZip)
            val bagManifestLocations = config.bagItConfig.digestNames
              .map((itemPath: String) =>
                BagItemPath(itemPath, bagRootPathInZip))

            ArchiveJob(
              externalIdentifier = externalIdentifier,
              zipFile = zipFile,
              maybeBagRootPathInZip = bagRootPathInZip,
              bagUploadLocation = BagLocation(
                storageNamespace = config.uploadConfig.uploadNamespace,
                storagePrefix = Some(config.uploadConfig.uploadPrefix),
                storageSpace = ingestBagRequest.storageSpace,
                bagPath = BagPath(externalIdentifier.toString)
              ),
              tagManifestLocation = tagManifestLocation,
              bagManifestLocations = bagManifestLocations,
              config = config.bagItConfig
            )
          }
      case Failure(e) =>
        Left(BagNotFoundError(e.getMessage, ingestBagRequest))
    }
  }

  /** The ZIP files contain a "bag-info.txt" metadata file, with
    * lines of the form:
    *
    *     Bagging-Date: 2018-08-24
    *     Contact-Name: Henry Wellcome
    *     External-Identifier: 1234
    *
    * This method extracts the "External-Identifier" field from this metadata,
    * if present.
    *
    * In the BagIt spec, all these fields are optional, so it may not exist --
    * but that's an error for our use case!  We use the External-Identifier to
    * determine where to store the bag in S3, so its absence means we're
    * unable to proceed.
    *
    */
  private def getBagIdentifier(bagIdentifierZipEntryPointer: ZipEntryPointer,
                               ingestBagRequest: IngestBagRequest)
    : Either[ArchiveError[IngestBagRequest], ExternalIdentifier] = {
    ZipFileReader
      .maybeInputStream(bagIdentifierZipEntryPointer)
      .toRight[ArchiveError[IngestBagRequest]](
        FileNotFoundError(
          bagIdentifierZipEntryPointer.zipPath,
          ingestBagRequest))
      .flatMap { inputStream =>
        BagInfoParser
          .parseBagInfo(ingestBagRequest, inputStream)
          .map(_.externalIdentifier)
      }
  }
}
