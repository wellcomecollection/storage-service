package uk.ac.wellcome.platform.archive.archivist.bag

import java.io.InputStream

import cats.implicits._
import uk.ac.wellcome.platform.archive.archivist.models.errors.FileNotFoundError
import uk.ac.wellcome.platform.archive.archivist.models._
import uk.ac.wellcome.platform.archive.archivist.zipfile.ZipFileReader
import uk.ac.wellcome.platform.archive.common.bag.BagDigestFileCreator
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagDigestFile, BagItemPath}
import uk.ac.wellcome.platform.archive.common.models.error.ArchiveError

object ArchiveItemJobCreator {
  /** Returns a list of all the items inside a bag that the manifest(s)
    * refer to.
    *
    * If any of the manifests are incorrectly formatted, it returns an error.
    *
    */
  def createArchiveDigestItemJobs(job: ArchiveJob)
    : Either[ArchiveError[ArchiveJob], List[DigestItemJob]] =
    job.bagManifestLocations
      .map { manifestLocation: BagItemPath =>
        ZipEntryPointer(
          zipFile = job.zipFile,
          zipPath = manifestLocation.underlying
        )
      }
      .traverse { zipEntryPointer =>
        createDigestItemJobs(job, zipEntryPointer)
      }
      .map { _.flatten }

  /** Given the location of a single manifest inside the BagIt bag,
    * return a list of all the items inside the bag that the manifest
    * refers to.
    *
    * If the manifest is incorrectly formatted, it returns an error.
    *
    */
  private def createDigestItemJobs(job: ArchiveJob,
                                   manifestZipEntryPointer: ZipEntryPointer)
    : Either[ArchiveError[ArchiveJob], List[DigestItemJob]] = {
    val value: Either[ArchiveError[ArchiveJob], InputStream] =
      ZipFileReader
        .maybeInputStream(manifestZipEntryPointer)
        .toRight(FileNotFoundError(manifestZipEntryPointer.zipPath, job))

    value.flatMap { manifestInputStream =>
      val manifestFileLines: List[String] =
        scala.io.Source
          .fromInputStream(manifestInputStream)
          .mkString
          .split("\n")
          .toList
      manifestFileLines
        .filter { _.nonEmpty }
        .traverse { manifestLine =>
          BagDigestFileCreator
            .create(
              manifestLine.trim(),
              job,
              job.maybeBagRootPathInZip,
              manifestZipEntryPointer.zipPath)
            .map { bagDigestFile =>
              DigestItemJob(
                archiveJob = job,
                zipEntryPointer = ZipEntryPointer(
                  zipFile = job.zipFile,
                  zipPath = bagDigestFile.path.underlying
                ),
                uploadLocation = UploadLocationBuilder.create(
                  bagUploadLocation = job.bagUploadLocation,
                  bagPathInZip = bagDigestFile.path.underlying,
                  maybeBagRootPathInZip = job.maybeBagRootPathInZip
                ),
                digest = bagDigestFile.checksum
              )
            }
        }
    }
  }

}
