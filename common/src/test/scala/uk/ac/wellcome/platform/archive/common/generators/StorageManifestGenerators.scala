package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagInfo, BagVersion}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.DestinationBuilder
import uk.ac.wellcome.platform.archive.common.verify.{HashingAlgorithm, SHA256}

import scala.util.Random

trait StorageManifestGenerators
    extends BagInfoGenerators
    with StorageSpaceGenerators
    with StorageLocationGenerators {

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
    createdDate: Instant = Instant.now,
    files: List[StorageManifestFile] = Nil
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
      location = createPrimaryLocation,
      replicaLocations = (1 to randomInt(0, 5))
        .map { _ =>
          createSecondaryLocation
        },
      createdDate = createdDate,
      ingestId = ingestId
    )
  }

  def createStorageManifestWithFileCount(fileCount: Int): StorageManifest =
    createStorageManifestWith(fileCount = fileCount)

  def createStorageManifest: StorageManifest =
    createStorageManifestWith()
}
