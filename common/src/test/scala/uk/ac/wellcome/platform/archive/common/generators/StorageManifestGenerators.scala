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
    with BagFileGenerators
    with StorageSpaceGenerators
    with ObjectLocationGenerators {

  val checksumAlgorithm: HashingAlgorithm = SHA256

  private def createStorageManifestFile: StorageManifestFile = {
    val bagFile = createBagFile
    StorageManifestFile(
      checksum = bagFile.checksum.value,
      name = bagFile.path.value,
      path = bagFile.path.value,
      size = Random.nextLong().abs
    )
  }

  def createStorageManifestWith(
    ingestId: IngestID = createIngestID,
    space: StorageSpace = createStorageSpace,
    bagInfo: BagInfo = createBagInfo,
    version: BagVersion = BagVersion(Random.nextInt),
    fileCount: Int = 3
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
      location = PrimaryStorageLocation(
        provider = StandardStorageProvider,
        prefix = createObjectLocationPrefix
      ),
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
    createStorageManifestWith(fileCount = fileCount)

  def createStorageManifest: StorageManifest =
    createStorageManifestWith()
}
