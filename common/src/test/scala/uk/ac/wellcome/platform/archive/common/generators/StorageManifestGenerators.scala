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
    pathPrefix: String
  ): StorageManifestFile = {

    val name  = randomAlphanumeric
    val path = createBagPathWithPrefix(pathPrefix, name)

    StorageManifestFile(
      checksum = randomChecksumValue,
      name = name,
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
  ): StorageManifest = {

    val destinationBuilder = new DestinationBuilder(randomAlphanumeric)

    val destination = destinationBuilder.buildDestination(
      storageSpace=space,
      externalIdentifier = bagInfo.externalIdentifier,
      version = version
    )

    val pathPrefix = destination.path

    StorageManifest(
      space = space,
      info = bagInfo,
      version = version,
      manifest = FileManifest(
        checksumAlgorithm,
        files = (1 to fileCount)
          .map(_ => createStorageManifestFileWith(pathPrefix))
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
