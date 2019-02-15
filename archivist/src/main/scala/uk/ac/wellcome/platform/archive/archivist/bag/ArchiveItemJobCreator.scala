package uk.ac.wellcome.platform.archive.archivist.bag

import java.io.InputStream

import cats.implicits._
import uk.ac.wellcome.platform.archive.archivist.models.errors.FileNotFoundError
import uk.ac.wellcome.platform.archive.archivist.models._
import uk.ac.wellcome.platform.archive.archivist.zipfile.ZipFileReader
import uk.ac.wellcome.platform.archive.common.bag.BagDigestFileCreator
import uk.ac.wellcome.platform.archive.common.models.bagit.BagItemPath
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
                                   zipEntryPointer: ZipEntryPointer)
    : Either[ArchiveError[ArchiveJob], List[DigestItemJob]] = {
    val value: Either[ArchiveError[ArchiveJob], InputStream] =
      ZipFileReader
        .maybeInputStream(zipEntryPointer)
        .toRight(FileNotFoundError(zipEntryPointer.zipPath, job))

    value.flatMap { inputStream =>
      val manifestFileLines: List[String] =
        scala.io.Source
          .fromInputStream(inputStream)
          .mkString
          .split("\n")
          .toList
      manifestFileLines
        .filter { _.nonEmpty }
        .traverse { line =>
          BagDigestFileCreator
            .create(
              line.trim(),
              job,
              job.maybeBagRootPathInZip,
              zipEntryPointer.zipPath)
            .map { bagItem =>
              DigestItemJob(
                archiveJob = job,
                zipEntryPointer = zipEntryPointer,
                uploadLocation = UploadLocationBuilder.create(
                  bagUploadLocation = job.bagUploadLocation,
                  bagPathInZip = bagItem.path.underlying,
                  maybeBagRootPathInZip = job.maybeBagRootPathInZip
                ),
                digest = bagItem.checksum
              )
            }
        }
    }
  }

}
