package uk.ac.wellcome.platform.archive.archivist.generators

import java.io.File
import java.util.zip.ZipFile

import uk.ac.wellcome.platform.archive.archivist.builders.UploadLocationBuilder
import uk.ac.wellcome.platform.archive.archivist.models._
import uk.ac.wellcome.platform.archive.common.generators.BagLocationGenerators
import uk.ac.wellcome.platform.archive.common.models.bagit._
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait ArchiveJobGenerators extends BagLocationGenerators {

  def createTagManifestItemJobWith(
    file: File,
    bucket: S3.Bucket = randomBucket,
    bagIdentifier: ExternalIdentifier = createExternalIdentifier,
    itemPath: String = randomAlphanumeric()): TagManifestItemJob = {
    val archiveJob = createArchiveJobWith(file, bagIdentifier, bucket)
    TagManifestItemJob(
      archiveJob = archiveJob,
      zipEntryPointer = ZipEntryPointer(
        zipFile = archiveJob.zipFile,
        zipPath = itemPath
      ),
      uploadLocation = UploadLocationBuilder.create(
        archiveJob.bagUploadLocation,
        itemPath
      )
    )
  }

  def createDigestItemJobWith(
    file: File,
    bucket: S3.Bucket,
    digest: String = randomAlphanumeric(),
    bagIdentifier: ExternalIdentifier = createExternalIdentifier,
    itemPath: String = randomAlphanumeric()): DigestItemJob = {
    val archiveJob = createArchiveJobWith(
      file = file,
      bagIdentifier = bagIdentifier,
      bucket = bucket
    )
    DigestItemJob(
      archiveJob = archiveJob,
      zipEntryPointer = ZipEntryPointer(
        zipFile = archiveJob.zipFile,
        zipPath = itemPath
      ),
      uploadLocation = UploadLocationBuilder.create(
        archiveJob.bagUploadLocation,
        itemPath
      ),
      digest = digest
    )
  }

  // todo
  def createArchiveJobWith(
    file: File,
    bagIdentifier: ExternalIdentifier = createExternalIdentifier,
    bucket: Bucket,
  ): ArchiveJob = {
    val bagLocation = createBagLocationWith(
      bucket = bucket,
      storagePrefix = Some("archive"),
      bagIdentifier = bagIdentifier
    )

    ArchiveJob(
      externalIdentifier = bagIdentifier,
      zipFile = new ZipFile(file),
      bagUploadLocation = bagLocation,
      tagManifestLocation = BagItemPath("tagmanifest-sha256.txt"),
      bagManifestLocations = List(
        BagItemPath("manifest-sha256.txt"),
        BagItemPath("tagmanifest-sha256.txt")
      ),
      config = BagItConfig()
    )
  }
}
