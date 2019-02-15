package uk.ac.wellcome.platform.archive.archivist.generators

import java.io.File
import java.util.zip.ZipFile

import uk.ac.wellcome.platform.archive.archivist.models._
import uk.ac.wellcome.platform.archive.common.generators.{ExternalIdentifierGenerators, StorageSpaceGenerators}
import uk.ac.wellcome.platform.archive.common.models.bagit._
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait ArchiveJobGenerators
    extends ExternalIdentifierGenerators
    with StorageSpaceGenerators {

  def createArchiveItemJobWith( file: File,
                                bucket: S3.Bucket = randomBucket,
                                bagIdentifier: ExternalIdentifier = createExternalIdentifier,
                                itemPath: String = randomAlphanumeric()
  ): TagManifestItemJob = {
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


  def createArchiveDigestItemJobWith( file: File,
                                      bucket: S3.Bucket,
                                      digest: String = randomAlphanumeric(),
                                      bagIdentifier: ExternalIdentifier = createExternalIdentifier,
                                      itemPath: String = randomAlphanumeric()
  ): DigestItemJob = {
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

  def createArchiveJobWithA(bagIdentifier: ExternalIdentifier,
                            file: File,
                            maybeBagRootPathInZip: Option[String] = None,
                            bagUploadLocation: BagLocation,
                            tagManifestLocation: BagItemPath = BagItemPath("tagmanifest-sha256.txt"),
                            bagManifestLocations: List[BagItemPath] = List(
                             BagItemPath("manifest-sha256.txt"),
                             BagItemPath("tagmanifest-sha256.txt")
                           ),
                            config: BagItConfig = BagItConfig()
                          ): ArchiveJob = {
    ArchiveJob(
      externalIdentifier = bagIdentifier,
      zipFile = new ZipFile(file),
      maybeBagRootPathInZip = maybeBagRootPathInZip,
      bagUploadLocation = bagUploadLocation,
      tagManifestLocation = tagManifestLocation,
      bagManifestLocations = bagManifestLocations,
      config = config
    )
  }

  // todo
  def createArchiveJobWith(
    file: File,
    bagIdentifier: ExternalIdentifier = createExternalIdentifier,
    bucket: Bucket,
  ): ArchiveJob = {
    val bagLocation = BagLocation(
      storageNamespace = bucket.name,
      storagePrefix = "archive",
      storageSpace = createStorageSpace,
      bagPath = BagPath(bagIdentifier.toString)
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
