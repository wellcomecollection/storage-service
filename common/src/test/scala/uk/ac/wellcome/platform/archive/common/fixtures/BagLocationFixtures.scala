package uk.ac.wellcome.platform.archive.common.fixtures

import java.nio.file.Paths
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  BagLocation,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait BagLocationFixtures
    extends S3
    with BagInfoGenerators
    with BagIt
    with StorageSpaceGenerators {

  def withBag[R](
    bucket: Bucket,
    bagInfo: BagInfo = createBagInfo,
    dataFileCount: Int = 1,
    storageSpace: StorageSpace = createStorageSpace,
    createDataManifest: List[(String, String)] => Option[FileEntry] =
      createValidDataManifest,
    createTagManifest: List[(String, String)] => Option[FileEntry] =
      createValidTagManifest,
    bagRootDirectory: Option[String] = None)(
    testWith: TestWith[BagLocation, R]): R = {
    val bagIdentifier = createExternalIdentifier

    info(s"Creating bag $bagIdentifier")

    val fileEntries = createBag(
      bagInfo,
      dataFileCount = dataFileCount,
      createDataManifest = createDataManifest,
      createTagManifest = createTagManifest)

    val bagLocation = BagLocation(
      storageNamespace = bucket.name,
      storagePrefix = None,
      storageSpace = storageSpace,
      bagPath = BagPath(bagIdentifier.toString)
    )

    fileEntries.map((entry: FileEntry) => {
      s3Client
        .putObject(
          bagLocation.storageNamespace,
          Paths
            .get(
              bagLocation.completePath,
              bagRootDirectory.getOrElse(""),
              entry.name)
            .toString,
          entry.contents
        )
    })

    testWith(bagLocation)
  }
}
