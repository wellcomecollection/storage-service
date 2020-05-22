package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagInfo, BagVersion}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models._
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

  // TODO:
  //  the path prefix needs to be what the DestinationBuilder
  //  provides - should those be coupled here somehow
  def createStorageManifestFileWith(
    pathPrefix: String
  ): StorageManifestFile = {
    val path = createBagPathWithPrefix(pathPrefix)

    StorageManifestFile(
      checksum = randomChecksumValue,
      name = path.value,
      path = path.value,
      size = Random.nextLong().abs
    )
  }

  def createStorageManifestWith(
    ingestId: IngestID = createIngestID,
    space: StorageSpace = createStorageSpace,
    bagInfo: BagInfo = createBagInfo,
    version: BagVersion = createBagVersion,
    fileCount: Int = 3,
    createdDate: Instant = Instant.now
  ): StorageManifest =
    StorageManifest(
      space = space,
      info = bagInfo,
      version = version,
      manifest = FileManifest(
        checksumAlgorithm,
        files = (1 to fileCount)
          .map(_ => createStorageManifestFile)
      ),
      tagManifest = FileManifest(
        checksumAlgorithm,
        files = Seq(
          createStorageManifestFile,
          createStorageManifestFile,
          createStorageManifestFile
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

  def createStorageManifestWithFileCount(fileCount: Int): StorageManifest =
    createStorageManifestWith(fileCount = fileCount)

  def createStorageManifest: StorageManifest =
    createStorageManifestWith()
}
