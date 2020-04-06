package uk.ac.wellcome.platform.archive.common.generators

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagInfo, BagVersion}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  IngestID,
  StandardStorageProvider
}
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.verify.{HashingAlgorithm, SHA256}
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

import scala.util.Random

trait StorageManifestGenerators
    extends BagInfoGenerators
    with StorageSpaceGenerators
    with ObjectLocationGenerators {

  val checksumAlgorithm: HashingAlgorithm = SHA256

  def createStorageManifestFileWith(
    path: String = randomAlphanumeric,
    size: Long = Random.nextLong().abs
  ): StorageManifestFile =
    StorageManifestFile(
      checksum = randomChecksumValue,
      name = randomAlphanumeric,
      path = path,
      size = size
    )

  private def createStorageManifestFile: StorageManifestFile =
    createStorageManifestFileWith()

  def createStorageManifestWith(
    ingestId: IngestID = createIngestID,
    space: StorageSpace = createStorageSpace,
    bagInfo: BagInfo = createBagInfo,
    version: BagVersion = createBagVersion,
    manifestFiles: Seq[StorageManifestFile] = (1 to 3).map { _ =>
      createStorageManifestFile
    },
    location: StorageLocation = PrimaryStorageLocation(
      provider = StandardStorageProvider,
      prefix = createObjectLocationPrefix
    )
  ): StorageManifest =
    StorageManifest(
      space = space,
      info = bagInfo,
      version = version,
      manifest = FileManifest(
        checksumAlgorithm,
        files = manifestFiles
      ),
      tagManifest = FileManifest(
        checksumAlgorithm,
        files = Seq(
          createStorageManifestFile,
          createStorageManifestFile,
          createStorageManifestFile
        )
      ),
      location = location,
      replicaLocations = (1 to randomInt(0, 5))
        .map { _ =>
          SecondaryStorageLocation(
            provider = StandardStorageProvider,
            prefix = createObjectLocationPrefix
          )
        },
      createdDate = Instant.now,
      ingestId = ingestId
    )

  def createStorageManifestWithFileCount(fileCount: Int): StorageManifest =
    createStorageManifestWith(
      manifestFiles = (1 to fileCount).map { _ =>
        createStorageManifestFile
      }
    )

  def createStorageManifest: StorageManifest =
    createStorageManifestWith()
}
