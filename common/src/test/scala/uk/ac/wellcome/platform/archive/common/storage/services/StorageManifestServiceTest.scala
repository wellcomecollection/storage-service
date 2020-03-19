package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest._
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagPath,
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.services.memory.MemoryBagReader
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagBuilder,
  BagBuilderBase
}
import uk.ac.wellcome.platform.archive.common.generators._
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryStorageLocation,
  SecondaryStorageLocation,
  StorageManifest,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.verify.{MD5, SHA256}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.memory.{MemoryStreamStore, MemoryTypedStore}

import scala.util.{Failure, Random, Success, Try}

class StorageManifestServiceTest
    extends FunSpec
    with Matchers
    with BagGenerators
    with BagFileGenerators
    with FetchMetadataGenerators
    with StorageLocationGenerators
    with StorageSpaceGenerators
    with TimeTestFixture
    with EitherValues
    with TryValues {

  describe("checks the replica root paths") {
    describe("primary location") {
      it("fails if the root is not a versioned directory") {
        val location = createPrimaryLocationWith(
          prefix = createObjectLocationPrefix
        )
        val version = createBagVersion

        assertIsError(location = location, version = version) { err =>
          err shouldBe a[StorageManifestException]
          err.getMessage shouldBe s"Malformed bag root: ${location.prefix.namespace}/${location.prefix.path} (expected suffix /$version)"
        }
      }

      it("fails if the root has the wrong version") {
        val version = createBagVersion
        val location = createPrimaryLocationWith(version = version.increment)

        assertIsError(location = location, version = version) { err =>
          err shouldBe a[StorageManifestException]
          err.getMessage shouldBe s"Malformed bag root: ${location.prefix.namespace}/${location.prefix.path} (expected suffix /$version)"
        }
      }
    }

    describe("secondary location") {
      it("fails if the root is not a versioned directory") {
        val replicas = Seq(
          createSecondaryLocation,
          createSecondaryLocation
        )

        assertIsError(replicas = replicas) { err =>
          err shouldBe a[StorageManifestException]
          err.getMessage should startWith("Malformed bag root in the replicas:")
        }
      }

      it("fails if the root has the wrong version") {
        val location = createPrimaryLocationWith(version = BagVersion(1))
        val version = BagVersion(1)

        val replicas = Seq(
          createSecondaryLocationWith(BagVersion(2)),
          createSecondaryLocationWith(BagVersion(3))
        )

        assertIsError(
          location = location,
          replicas = replicas,
          version = version
        ) { err =>
          err shouldBe a[StorageManifestException]
          err.getMessage should startWith("Malformed bag root in the replicas:")
        }
      }
    }
  }

  describe("sets the locations and replicaLocations correctly") {
    val version = createBagVersion

    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val (bagRoot, bag) = createStorageManifestBag(version = version)

    val location = createPrimaryLocationWith(
      prefix = bagRoot
    )

    val replicas = collectionOf(max = 10) {
      createSecondaryLocationWith(version)
    }

    val storageManifest = createManifest(
      bag = bag,
      location = location,
      replicas = replicas,
      version = version
    )

    it("sets the correct provider on the primary location") {
      storageManifest.location.provider shouldBe location.provider
    }

    it("sets the correct prefix on the primary location") {
      storageManifest.location.prefix shouldBe bagRoot.copy(
        path = bagRoot.path.stripSuffix(s"/$version")
      )
    }

    it("uses the correct providers on the replica locations") {
      val expectedProviders = replicas.map { _.provider }
      val actualProviders = storageManifest.replicaLocations.map { _.provider }

      actualProviders shouldBe expectedProviders
    }

    it("uses the correct roots on the replica locations") {
      val expectedPrefixes = replicas
        .map { _.prefix }
        .map { prefix =>
          prefix.copy(
            path = prefix.path.stripSuffix(s"/$version")
          )
        }

      val actualPrefixes = storageManifest.replicaLocations.map { _.prefix }

      actualPrefixes shouldBe expectedPrefixes
    }
  }

  describe("if there's no fetch.txt, it versions the paths") {
    object NoFetchBagBuilder extends BagBuilderBase {
      override protected def getFetchEntryCount(payloadFileCount: Int): Int = 0
    }

    val version = createBagVersion

    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val (bagRoot, bag) = createStorageManifestBag(
      version = version,
      bagBuilder = NoFetchBagBuilder
    )

    val location = createPrimaryLocationWith(prefix = bagRoot)

    val storageManifest = createManifest(
      bag = bag,
      location = location,
      version = version
    )

    it("manifest entries") {
      storageManifest.manifest.files
        .foreach { file =>
          file.path shouldBe s"$version/${file.name}"
        }
    }

    it("tag manifest entries") {
      storageManifest.tagManifest.files
        .foreach { file =>
          file.path shouldBe s"$version/${file.name}"
        }
    }
  }

  describe("if there's a fetch.txt, it constructs the right paths") {
    object AtLeastOneFetchEntryBagBuilder extends BagBuilderBase {
      override protected def getFetchEntryCount(payloadFileCount: Int): Int =
        randomInt(from = 1, to = payloadFileCount)
    }

    val version = createBagVersion

    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val (bagRoot, bag) = createStorageManifestBag(
      version = version,
      bagBuilder = AtLeastOneFetchEntryBagBuilder
    )

    val location = createPrimaryLocationWith(prefix = bagRoot)

    val storageManifest = createManifest(
      bag = bag,
      location = location,
      version = version
    )

    val bagFetchEntries = bag.fetch.get.entries

    it("puts fetched entries under a versioned path") {
      val fetchedFiles = storageManifest.manifest.files
        .filter { file => bagFetchEntries.contains(BagPath(file.name)) }

      fetchedFiles should not be empty

      fetchedFiles
        .foreach { file =>
          val fetchEntry = bagFetchEntries(BagPath(file.name))

          // The fetch entry URI is of the form
          //
          //    s3://{bucket}/{space}/{externalIdentifier}/{version}/{path_inside_bag}
          //
          // We want to check that matches the bag path, which is of the form
          //
          //    {version}/{path_inside_bag}
          //
          fetchEntry.uri.toString should endWith(file.path)
          file.path.matches("^v\\d+/")
        }
    }

    it("puts non-fetched entries under the current version") {
      storageManifest.manifest.files
        .filterNot { file =>
          bagFetchEntries.contains(BagPath(file.name))
        }
        .foreach { file =>
          file.path shouldBe s"$version/${file.name}"
        }
    }

    it("tag manifest entries are always under the current version") {
      storageManifest.tagManifest.files
        .foreach { file =>
          file.path shouldBe s"$version/${file.name}"
        }
    }
  }

  describe("validates the checksums") {
    val version = createBagVersion

    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val (bagRoot, bag) = createStorageManifestBag(
      version = version
    )

    val location = createPrimaryLocationWith(prefix = bagRoot)

    val storageManifest = createManifest(
      bag = bag,
      location = location,
      version = version
    )

    it("uses the checksum values from the file manifest") {
      val storageManifestChecksums =
        storageManifest.manifest.files.map { file =>
          file.name -> file.checksum.value
        }.toMap

      val bagChecksums =
        bag.manifest.entries
          .map { case (bagPath, checksum) => bagPath.value -> checksum.value.value }

      storageManifestChecksums shouldBe bagChecksums
    }

    it("uses the checksum values from the tag manifest") {
      val storageManifestChecksums =
        storageManifest.tagManifest.files.map { file =>
          file.name -> file.checksum.value
        }.toMap

      val bagChecksums =
        bag.tagManifest.entries
          .map { case (bagPath, checksum) => bagPath.value -> checksum.value.value }

      storageManifestChecksums.filterKeys { _ != "tagmanifest-sha256.txt" } shouldBe bagChecksums
    }

    it(
      "fails if one of the file manifest entries has the wrong hashing algorithm"
    ) {
      val badPath = randomAlphanumeric
      val manifestFiles = Seq(
        createBagFileWith(
          path = badPath,
          checksumAlgorithm = MD5
        )
      )

      val bag = createBagWith(
        manifestFiles = manifestFiles,
        manifestChecksumAlgorithm = SHA256
      )

      assertIsError(bag = bag) { err =>
        err.getMessage shouldBe s"Mismatched checksum algorithms in manifest: entry $badPath has algorithm MD5, but manifest uses SHA-256"
      }
    }

    it(
      "fails if one of the tag manifest entries has the wrong hashing algorithm"
    ) {
      // TODO: Rewrite this to use generators
      val badPath = randomAlphanumeric
      val tagManifestFiles = Seq(
        createBagFileWith(
          path = badPath,
          checksumAlgorithm = MD5
        )
      )

      val bag = createBagWith(
        tagManifestFiles = tagManifestFiles,
        tagManifestChecksumAlgorithm = SHA256
      )

      assertIsError(bag = bag) { err =>
        err shouldBe a[StorageManifestException]
        err.getMessage shouldBe s"Mismatched checksum algorithms in manifest: entry $badPath has algorithm MD5, but manifest uses SHA-256"
      }
    }
  }

  describe("fails if the fetch.txt is wrong") {
    it("refers to files that aren't in the manifest") {
      val fetchEntries = Map(
        BagPath(randomAlphanumeric) -> createFetchMetadata
      )

      val bag = createBagWith(
        fetchEntries = fetchEntries
      )

      assertIsError(bag = bag) { err =>
        err shouldBe a[StorageManifestException]
        err.getMessage should startWith("Unable to resolve fetch entries:")
        err.getMessage should include(
          s"fetch.txt refers to paths that aren't in the bag manifest:"
        )
      }
    }

    it("refers to a file in the wrong namespace") {
      val fetchEntries = Map(
        BagPath("data/file1.txt") -> createFetchMetadataWith(
          uri = "s3://not-the-replica-bucket/file1.txt"
        )
      )

      val bag = createBagWith(
        manifestFiles = Seq(
          createBagFileWith("data/file1.txt")
        ),
        fetchEntries = fetchEntries
      )

      assertIsError(bag = bag) { err =>
        err shouldBe a[BadFetchLocationException]
        err.getMessage shouldBe "Fetch entry for data/file1.txt refers to a file in the wrong namespace: not-the-replica-bucket"
      }
    }

    it("refers to a file that isn't in a versioned directory") {
      val version = createBagVersion
      val bagRoot = createObjectLocation

      val location = createPrimaryLocationWith(
        bagRoot = bagRoot,
        version = version
      )

      val fetchEntries = Map(
        BagPath("data/file1.txt") -> createFetchMetadataWith(
          uri = s"s3://${location.prefix.namespace}/file1.txt"
        )
      )

      val bag = createBagWith(
        manifestFiles = Seq(
          createBagFileWith("data/file1.txt")
        ),
        fetchEntries = fetchEntries
      )

      assertIsError(bag = bag, location = location, version = version) { err =>
        err shouldBe a[BadFetchLocationException]
        err.getMessage shouldBe "Fetch entry for data/file1.txt refers to a file in the wrong path: /file1.txt"
      }
    }
  }

  describe("passes through metadata correctly") {
    val version = createBagVersion
    val space = createStorageSpace

    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val (bagRoot, bag) = createStorageManifestBag(
      space = space,
      version = version
    )

    val location = createPrimaryLocationWith(prefix = bagRoot)

    val storageManifest = createManifest(
      bag = bag,
      location = location,
      space = space,
      version = version
    )

    it("sets the correct storage space") {
      storageManifest.space shouldBe space
    }

    it("sets the correct bagInfo") {
      storageManifest.info shouldBe bag.info
    }

    it("sets the correct version") {
      storageManifest.version shouldBe version
    }

    it("sets a recent createdDate") {
      // This test takes longer when running on a Mac, not in CI, so allow some
      // flex on the definition of "recent".
      assertRecent(storageManifest.createdDate, recentSeconds = 30)
    }
  }

  describe("adds the size to the manifest files") {
    it("fails if it cannot get the size of a file") {
      val version = createBagVersion
      val location = createPrimaryLocationWith(
        version = version
      )

      val files = Seq("data/file1.txt", "data/file2.txt", "data/dir/file3.txt")

      val bag = createBagWith(
        manifestFiles = files.map { path =>
          createBagFileWith(path)
        }
      )

      val err = new Throwable("BOOM!")

      val brokenSizeFinder = new SizeFinder {
        override def getSize(location: ObjectLocation): Try[Long] =
          Failure(err)
      }

      implicit val streamStore: MemoryStreamStore[ObjectLocation] =
        MemoryStreamStore[ObjectLocation]()

      val service = new StorageManifestService(brokenSizeFinder)

      val result = service.createManifest(
        ingestId = createIngestID,
        bag = bag,
        location = location,
        replicas = Seq.empty,
        space = createStorageSpace,
        version = version
      )

      result.failed.get shouldBe a[StorageManifestException]
      result.failed.get.getMessage should startWith(
        s"Error getting size of ${location.prefix.asLocation("data/file1.txt")}"
      )
    }

    it("uses the size finder to get sizes") {
      object NoFetchBagBuilder extends BagBuilderBase {
        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          0
      }

      val version = createBagVersion

      implicit val streamStore: MemoryStreamStore[ObjectLocation] =
        MemoryStreamStore[ObjectLocation]()

      val (bagRoot, bag) = createStorageManifestBag(
        version = version,
        bagBuilder = NoFetchBagBuilder
      )

      val location = createPrimaryLocationWith(prefix = bagRoot)

      var sizeCache: Map[ObjectLocation, Long] = Map.empty

      val cachingSizeFinder = new SizeFinder {
        override def getSize(location: ObjectLocation): Try[Long] = Try {
          sizeCache = sizeCache + (location -> Random.nextLong())
          sizeCache(location)
        }
      }

      val storageManifest = createManifest(
        bag = bag.copy(
          tagManifest = bag.tagManifest.copy(files = Seq.empty)
        ),
        location = location,
        version = version,
        sizeFinder = cachingSizeFinder
      )

      val storageManifestSizes =
        (storageManifest.manifest.files ++ storageManifest.tagManifest.files)
          .filterNot {
            // The size of this tag manifest is fetched when we read the file contents,
            // not from the size finder.
            _.name == "tagmanifest-sha256.txt"
          }
          .map { file =>
            storageManifest.location.prefix.asLocation(file.path) -> file.size
          }
          .toMap

      storageManifestSizes shouldBe sizeCache
    }

    it("uses the size from the fetch file") {
      // Always create a fetch.txt entry rather than a concrete file, always
      // include the size in the fetch.txt.
      object ConcreteFetchEntryBagBuilder extends BagBuilderBase {
        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount

        override protected def buildFetchEntryLine(
          entry: PayloadEntry
        )(implicit namespace: String): String =
          s"""bag://$namespace/${entry.path} ${entry.contents.getBytes.length} ${entry.bagPath}"""
      }

      val version = createBagVersion

      implicit val streamStore: MemoryStreamStore[ObjectLocation] =
        MemoryStreamStore[ObjectLocation]()

      val (bagRoot, bag) = createStorageManifestBag(
        version = version,
        bagBuilder = ConcreteFetchEntryBagBuilder
      )

      val location = createPrimaryLocationWith(prefix = bagRoot)

      val brokenSizeFinder = new SizeFinder {
        override def getSize(location: ObjectLocation): Try[Long] =
          Failure(new Throwable("This should never be called!"))
      }

      val storageManifest = createManifest(
        bag = bag.copy(
          tagManifest = bag.tagManifest.copy(files = Seq.empty)
        ),
        location = location,
        version = version,
        sizeFinder = brokenSizeFinder
      )

      val manifestSizes =
        storageManifest.manifest.files.map { file =>
          file.name -> file.size
        }.toMap

      val bagSizes = bag.fetch.get.entries.map {
        case (bagPath, fetchMetadata) =>
          bagPath.value -> fetchMetadata.length.get
      }

      manifestSizes shouldBe bagSizes
    }
  }

  def createStorageManifestBag(
    space: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: BagVersion,
    bagBuilder: BagBuilderBase = BagBuilder
  )(
    implicit
    namespace: String = randomAlphanumeric,
    streamStore: MemoryStreamStore[ObjectLocation]
  ): (ObjectLocationPrefix, Bag) = {
    implicit val typedStore: MemoryTypedStore[ObjectLocation, String] =
      new MemoryTypedStore[ObjectLocation, String]()

    val (bagObjects, bagRoot, _) =
      bagBuilder.createBagContentsWith(
        space = space,
        externalIdentifier = externalIdentifier,
        version = version
      )

    bagBuilder.uploadBagObjects(bagObjects)

    (bagRoot, new MemoryBagReader().get(bagRoot).right.value)
  }

  describe(
    "finds files that aren't referenced in the BagIt tagmanifest-sha256.txt"
  ) {
    it("finds BagIt tag manifest files") {
      implicit val streamStore: MemoryStreamStore[ObjectLocation] =
        MemoryStreamStore[ObjectLocation]()

      val space = createStorageSpace
      val externalIdentifier = createExternalIdentifier
      val version = createBagVersion

      val (bagRoot, bag) = createStorageManifestBag(
        space = space,
        externalIdentifier,
        version = version
      )

      val manifest = createManifest(
        bag = bag,
        location = createPrimaryLocationWith(prefix = bagRoot),
        space = space,
        version = version
      )

      val tagManifestFiles = manifest.tagManifest.files.filter {
        _.name.startsWith("tagmanifest-")
      }

      tagManifestFiles.isEmpty shouldBe false
    }
  }

  private def createManifest(
    ingestId: IngestID = createIngestID,
    bag: Bag,
    location: PrimaryStorageLocation,
    replicas: Seq[SecondaryStorageLocation] = Seq.empty,
    space: StorageSpace = createStorageSpace,
    version: BagVersion,
    sizeFinder: SizeFinder = (location: ObjectLocation) =>
      Success(Random.nextLong().abs)
  )(
    implicit streamStore: MemoryStreamStore[ObjectLocation]
  ): StorageManifest = {
    val service = new StorageManifestService(sizeFinder)

    val result = service.createManifest(
      ingestId = ingestId,
      bag = bag,
      location = location,
      replicas = replicas,
      space = space,
      version = version
    )

    if (result.isFailure) {
      println(result)
    }

    result.success.value
  }

  def createPrimaryLocationWith(version: BagVersion): PrimaryStorageLocation =
    createPrimaryLocationWith(
      bagRoot = createObjectLocation,
      version = version
    )

  def createPrimaryLocationWith(
    bagRoot: ObjectLocation,
    version: BagVersion
  ): PrimaryStorageLocation =
    createPrimaryLocationWith(
      prefix = bagRoot.join(version.toString).asPrefix
    )

  def createSecondaryLocationWith(
    version: BagVersion
  ): SecondaryStorageLocation =
    createSecondaryLocationWith(
      prefix = createObjectLocation.join(version.toString).asPrefix
    )

  private def assertIsError(
    ingestId: IngestID = createIngestID,
    bag: Bag = createBag,
    location: PrimaryStorageLocation = createPrimaryLocationWith(
      version = BagVersion(1)
    ),
    replicas: Seq[SecondaryStorageLocation] = Seq.empty,
    version: BagVersion = BagVersion(1)
  )(assertError: Throwable => Assertion): Assertion = {
    val sizeFinder = new SizeFinder {
      override def getSize(location: ObjectLocation): Try[Long] = Success(1)
    }

    implicit val streamStore: MemoryStreamStore[ObjectLocation] =
      MemoryStreamStore[ObjectLocation]()

    val service = new StorageManifestService(sizeFinder)

    val result = service.createManifest(
      ingestId = ingestId,
      bag = bag,
      location = location,
      replicas = replicas,
      space = createStorageSpace,
      version = version
    )

    assertError(result.failure.exception)
  }
}
