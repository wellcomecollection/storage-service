package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagInfo, BagVersion}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.DestinationBuilder
import uk.ac.wellcome.platform.archive.common.verify.{HashingAlgorithm, SHA256}
import uk.ac.wellcome.storage.azure.AzureBlobLocationPrefix

import scala.util.Random

trait StorageManifestGenerators
    extends BagInfoGenerators
    with StorageSpaceGenerators
    with ReplicaLocationGenerators {

  val checksumAlgorithm: HashingAlgorithm = SHA256

  def createStorageManifestFile: StorageManifestFile = {
    val path = createBagPath

    StorageManifestFile(
      checksum = randomChecksumValue,
      name = path.value,
      path = path.value,
      size = Random.nextLong().abs
    )
  }

  def createStorageManifestFileWith(
    pathPrefix: String = randomAlphanumeric,
    name: String = randomAlphanumeric,
    size: Long = Random.nextLong().abs
  ): StorageManifestFile = {

    val path = createBagPathWithPrefix(pathPrefix, name)

    StorageManifestFile(
      checksum = randomChecksumValue,
      name = name,
      path = path.value,
      size = size
    )
  }

  def createStorageManifestWith(
    ingestId: IngestID = createIngestID,
    space: StorageSpace = createStorageSpace,
    bagInfo: BagInfo = createBagInfo,
    version: BagVersion = createBagVersion,
    fileCount: Int = 3,
    location: PrimaryStorageLocation = PrimaryS3StorageLocation(
      createS3ObjectLocationPrefix
    ),
    replicaLocations: Seq[SecondaryStorageLocation] = (1 to randomInt(0, 5))
      .map { _ =>
        chooseFrom(
          Seq(
            SecondaryS3StorageLocation(createS3ObjectLocationPrefix),
            SecondaryAzureStorageLocation(
              AzureBlobLocationPrefix(
                randomAlphanumeric,
                randomAlphanumeric
              )
            )
          )
        )
      },
    createdDate: Instant = Instant.now,
    files: Seq[StorageManifestFile] = Nil
  ): StorageManifest = {

    val pathPrefix = DestinationBuilder
      .buildPath(
        space,
        bagInfo.externalIdentifier,
        version
      )

    val fileManifestFiles = if (files.isEmpty) {
      (1 to fileCount)
        .map(_ => createStorageManifestFileWith(pathPrefix))
    } else {
      files
    }

    StorageManifest(
      space = space,
      info = bagInfo,
      version = version,
      manifest = FileManifest(
        checksumAlgorithm,
        files = fileManifestFiles
      ),
      tagManifest = FileManifest(
        checksumAlgorithm,
        files = Seq(
          createStorageManifestFileWith(pathPrefix),
          createStorageManifestFileWith(pathPrefix),
          createStorageManifestFileWith(pathPrefix)
        )
      ),
      location = location,
      replicaLocations = replicaLocations,
      createdDate = createdDate,
      ingestId = ingestId
    )
  }

  def createStorageManifestWithFileCount(fileCount: Int): StorageManifest =
    createStorageManifestWith(fileCount = fileCount)

  def createStorageManifest: StorageManifest =
    createStorageManifestWith()
}
