package uk.ac.wellcome.platform.archive.common.fixtures

import java.net.URI

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagFetch,
  BagFetchEntry,
  BagInfo,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators
import uk.ac.wellcome.storage.store.{TypedStore, TypedStoreEntry}
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}

import scala.util.Random

trait BagLocationFixtures[Namespace]
    extends BagInfoGenerators
    with BagIt
    with StorageSpaceGenerators
    with ObjectLocationGenerators {
  def createObjectLocationWith(namespace: Namespace,
                               path: String): ObjectLocation

  def withBag[R](
    bagInfo: BagInfo = createBagInfo,
    dataFileCount: Int = 1,
    space: StorageSpace = createStorageSpace,
    createDataManifest: List[(String, String)] => Option[FileEntry] =
      createValidDataManifest,
    createTagManifest: List[(String, String)] => Option[FileEntry] =
      createValidTagManifest,
    bagRootDirectory: Option[String] = None)(
    testWith: TestWith[ObjectLocation, R])(
    implicit typedStore: TypedStore[ObjectLocation, String],
    namespace: Namespace
  ): R = {
    val externalIdentifier = bagInfo.externalIdentifier
    info(s"Creating Bag $externalIdentifier")

    val fileEntries = createBag(
      bagInfo,
      dataFileCount = dataFileCount,
      createDataManifest = createDataManifest,
      createTagManifest = createTagManifest)

    debug(s"fileEntries: $fileEntries")

    val storageSpaceRootLocation = createObjectLocationWith(
      namespace,
      path = space.toString
    )

    val bagRootLocation = storageSpaceRootLocation.join(
      externalIdentifier.toString
    )

    val unpackedBagLocation = bagRootLocation.join(
      bagRootDirectory.getOrElse("")
    )

    // To simulate a bag that contains both concrete objects and
    // fetch files, siphon off some of the entries to be written
    // as a fetch file.
    //
    // We need to make sure bag-info.txt, manifest.txt, and so on,
    // are written into the bag proper.
    val (realFiles, fetchFiles) = fileEntries.partition { file =>
      file.name.endsWith(".txt") || Random.nextFloat() < 0.8
    }

    realFiles.map { entry =>
      val entryLocation = unpackedBagLocation.join(entry.name)

      typedStore.put(entryLocation)(TypedStoreEntry(
        entry.contents,
        metadata = Map.empty)) shouldBe a[Right[_, _]]
    }

    val bagFetchEntries = fetchFiles.map { entry =>
      val entryLocation = createObjectLocationWith(
        namespace,
        path = randomAlphanumeric
      )

      typedStore.put(entryLocation)(TypedStoreEntry(
        entry.contents,
        metadata = Map.empty)) shouldBe a[Right[_, _]]

      BagFetchEntry(
        uri = new URI(s"s3://${entryLocation.namespace}/${entryLocation.path}"),
        length = Some(entry.contents.length),
        path = BagPath(entry.name)
      )
    }

    if (fetchFiles.nonEmpty) {
      val fetchLocation = unpackedBagLocation.join("fetch.txt")
      val fetchContents = BagFetch.write(bagFetchEntries)

      typedStore.put(fetchLocation)(TypedStoreEntry(
        fetchContents,
        metadata = Map.empty)) shouldBe a[Right[_, _]]
    }

    testWith(bagRootLocation)
  }
}

trait S3BagLocationFixtures
    extends S3Fixtures
    with BagLocationFixtures[Bucket] {

  implicit val s3StreamStore: S3StreamStore = new S3StreamStore()
  implicit val s3TypedStore: S3TypedStore[String] = new S3TypedStore[String]()

  def withS3Bag[R](
    bucket: Bucket,
    bagInfo: BagInfo = createBagInfo,
    dataFileCount: Int = 1,
    space: StorageSpace = createStorageSpace,
    createDataManifest: List[(String, String)] => Option[FileEntry] =
      createValidDataManifest,
    createTagManifest: List[(String, String)] => Option[FileEntry] =
      createValidTagManifest,
    bagRootDirectory: Option[String] = None)(
    testWith: TestWith[ObjectLocation, R]): R = {
    implicit val namespace: Bucket = bucket

    withBag(
      bagInfo = bagInfo,
      dataFileCount = dataFileCount,
      space = space,
      createDataManifest = createDataManifest,
      createTagManifest = createTagManifest,
      bagRootDirectory = bagRootDirectory
    ) { bagRootLocation =>
      testWith(bagRootLocation)
    }
  }
}
