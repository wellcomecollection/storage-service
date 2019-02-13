package uk.ac.wellcome.platform.archive.common.fixtures

import uk.ac.wellcome.platform.archive.common.models._
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagInfo, BagLocation, BagPath}
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.{BagInfoGenerators, StorageSpaceGenerators}

trait BagLocationFixtures extends S3 with BagInfoGenerators with BagIt with StorageSpaceGenerators {
  // TODO
  // def createBagLocationWith()

  def withBag[R](
    bucket: Bucket,
    dataFileCount: Int = 1,
    bagInfo: BagInfo = createBagInfo,
    storagePrefix: String = "archive",
    storageSpace: StorageSpace = createStorageSpace,
    createDataManifest: List[(String, String)] => Option[FileEntry] =
      createValidDataManifest,
    createTagManifest: List[(String, String)] => Option[FileEntry] =
      createValidTagManifest)(testWith: TestWith[BagLocation, R]): R = {
    val bagIdentifier = createExternalIdentifier

    info(s"Creating bag $bagIdentifier")

    val fileEntries = createBag(
      bagInfo,
      dataFileCount = dataFileCount,
      createDataManifest = createDataManifest,
      createTagManifest = createTagManifest)

    val bagLocation = BagLocation(
      storageNamespace = bucket.name,
      storagePrefix = storagePrefix,
      storageSpace = storageSpace,
      bagPath = BagPath(bagIdentifier.toString)
    )

    fileEntries.map((entry: FileEntry) => {
      s3Client
        .putObject(
          bagLocation.storageNamespace,
          s"${bagLocation.completePath}/${entry.name}",
          entry.contents
        )
    })

    testWith(bagLocation)
  }
}
