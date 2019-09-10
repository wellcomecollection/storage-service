package uk.ac.wellcome.platform.archive.common.storage.services

import java.net.URI

import org.scalatest.{Assertion, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, BagFetchEntry, BagPath, BagVersion}
import uk.ac.wellcome.platform.archive.common.generators.{BagFileGenerators, BagGenerators, StorageLocationGenerators, StorageSpaceGenerators}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryStorageLocation, SecondaryStorageLocation, StorageManifest, StorageSpace}
import uk.ac.wellcome.platform.archive.common.verify.{MD5, SHA256}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Random, Success, Try}

class StorageManifestServiceTest
    extends FunSpec
    with Matchers
    with BagGenerators
    with BagFileGenerators
    with StorageLocationGenerators
    with StorageSpaceGenerators
    with TimeTestFixture
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
    val bagRoot = createObjectLocation

    val location = createPrimaryLocationWith(
      bagRoot = bagRoot,
      version = version
    )

    val replicas = collectionOf(max = 10) {
      createSecondaryLocationWith(version)
    }

    val storageManifest = createManifest(
      location = location,
      replicas = replicas,
      version = version
    )

    it("sets the correct provider on the primary location") {
      storageManifest.location.provider shouldBe location.provider
    }

    it("sets the correct prefix on the primary location") {
      storageManifest.location.prefix shouldBe bagRoot.asPrefix
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

  describe("constructs the paths correctly") {
    it("no fetch.txt => all the file entries under a versioned path") {
      val version = createBagVersion
      val location = createPrimaryLocationWith(version = version)

      val files = Seq("data/file1.txt", "data/file2.txt", "data/dir/file3.txt")

      val bag = createBagWith(
        manifestFiles = files.map { path =>
          createBagFileWith(path = path)
        }
      )

      val storageManifest = createManifest(
        bag = bag,
        location = location,
        version = version
      )

      val namePathMap =
        storageManifest.manifest.files.map { file =>
          (file.name, file.path)
        }.toMap

      namePathMap shouldBe files.map { f =>
        (f, s"$version/$f")
      }.toMap
    }

    it("puts the tag manifest files under a versioned path") {
      val version = createBagVersion
      val location = createPrimaryLocationWith(version = version)

      val files =
        Seq("bag-info.txt", "tag-manifest-sha256.txt", "manifest-sha256.txt")

      val bag = createBagWith(
        tagManifestFiles = files.map { path =>
          createBagFileWith(path = path)
        }
      )

      val storageManifest = createManifest(
        bag = bag,
        location = location,
        version = version
      )

      val namePathMap =
        storageManifest.tagManifest.files.map { file =>
          (file.name, file.path)
        }.toMap

      namePathMap shouldBe files.map { f =>
        (f, s"$version/$f")
      }.toMap
    }

    it("puts a fetch entry under the right versioned path") {
      val version = createBagVersion
      val bagRoot = createObjectLocation

      val location = createPrimaryLocationWith(
        bagRoot = bagRoot,
        version = version
      )

      val fetchVersion = version.copy(underlying = version.underlying - 1)
      val fetchLocation = bagRoot.copy(
        path = s"${bagRoot.path}/$fetchVersion/data/file1.txt"
      )

      val fetchEntries = Seq(
        BagFetchEntry(
          uri =
            new URI(s"s3://${fetchLocation.namespace}/${fetchLocation.path}"),
          length = None,
          path = BagPath("data/file1.txt")
        )
      )

      val bag = createBagWith(
        manifestFiles = Seq(
          createBagFileWith("data/file1.txt")
        ),
        fetchEntries = fetchEntries
      )

      val storageManifest = createManifest(
        bag = bag,
        location = location,
        version = version
      )

      storageManifest.manifest.files.map { f =>
        f.name -> f.path
      }.toMap shouldBe Map("data/file1.txt" -> s"$fetchVersion/data/file1.txt")
    }

    it("uses a mixture of fetch entries and concrete files") {
      val version = createBagVersion
      val bagRoot = createObjectLocation

      val location = createPrimaryLocationWith(
        bagRoot = bagRoot,
        version = version
      )

      val fetchVersion = version.copy(underlying = version.underlying - 1)
      val fetchLocation = bagRoot.copy(
        path = s"${bagRoot.path}/$fetchVersion/data/file1.txt"
      )

      val fetchEntries = Seq(
        BagFetchEntry(
          uri =
            new URI(s"s3://${fetchLocation.namespace}/${fetchLocation.path}"),
          length = None,
          path = BagPath("data/file1.txt")
        )
      )

      val bag = createBagWith(
        manifestFiles = Seq(
          createBagFileWith("data/file1.txt"),
          createBagFileWith("data/file2.txt")
        ),
        fetchEntries = fetchEntries
      )

      val storageManifest = createManifest(
        bag = bag,
        location = location,
        version = version
      )

      storageManifest.manifest.files.map { f =>
        f.name -> f.path
      }.toMap shouldBe Map(
        "data/file1.txt" -> s"$fetchVersion/data/file1.txt",
        "data/file2.txt" -> s"$version/data/file2.txt"
      )
    }
  }

  describe("validates the checksums") {
    it("uses the checksum values from the file manifest") {
      val paths = Seq("data/file1.txt", "data/file2.txt", "data/dir/file3.txt")

      val filesWithChecksums =
        paths.map { _ -> randomAlphanumeric }

      val bag = createBagWith(
        manifestFiles = filesWithChecksums.map {
          case (path, checksumValue) =>
            createBagFileWith(path = path, checksum = checksumValue)
        }
      )

      val version = createBagVersion
      val location = createPrimaryLocationWith(
        version = version
      )

      val storageManifest = createManifest(
        bag = bag,
        location = location,
        version = version
      )

      val nameChecksumMap =
        storageManifest.manifest.files
          .map { file =>
            (file.name, file.checksum.value)
          }

      nameChecksumMap should contain theSameElementsAs filesWithChecksums
    }

    it("uses the checksum values from the tag manifest") {
      val paths =
        Seq("bag-info.txt", "tag-manifest-sha256.txt", "manifest-sha256.txt")

      val filesWithChecksums =
        paths.map { _ -> randomAlphanumeric }

      val bag = createBagWith(
        tagManifestFiles = filesWithChecksums.map {
          case (path, checksum) =>
            createBagFileWith(path = path, checksum = checksum)
        }
      )

      val version = createBagVersion
      val location = createPrimaryLocationWith(
        version = version
      )

      val storageManifest = createManifest(
        bag = bag,
        location = location,
        version = version
      )

      val nameChecksumMap =
        storageManifest.tagManifest.files
          .map { file =>
            (file.name, file.checksum.value)
          }

      nameChecksumMap should contain theSameElementsAs filesWithChecksums
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
      val fetchEntries = Seq(
        BagFetchEntry(
          uri = new URI("https://example.org/file1.txt"),
          length = None,
          path = BagPath(randomAlphanumeric)
        )
      )

      val bag = createBagWith(
        fetchEntries = fetchEntries
      )

      assertIsError(bag = bag) { err =>
        err shouldBe a[StorageManifestException]
        err.getMessage should startWith("Unable to resolve fetch entries:")
        err.getMessage should include(
          s"Fetch entry refers to a path that isn't in the bag manifest: ${fetchEntries.head.path}"
        )
      }
    }

    it("refers to a file in the wrong namespace") {
      val fetchEntries = Seq(
        BagFetchEntry(
          uri = new URI("s3://not-the-replica-bucket/file1.txt"),
          length = None,
          path = BagPath("data/file1.txt")
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

      val fetchEntries = Seq(
        BagFetchEntry(
          uri = new URI(s"s3://${location.prefix.namespace}/file1.txt"),
          length = None,
          path = BagPath("data/file1.txt")
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
    it("sets the correct storage space") {
      val space = createStorageSpace

      val manifest = createManifest(space = space)

      manifest.space shouldBe space
    }

    it("sets the correct bagInfo") {
      val bag = createBag

      val manifest = createManifest(bag = bag)

      manifest.info shouldBe bag.info
    }

    it("sets the correct version") {
      val version = createBagVersion
      val location = createPrimaryLocationWith(
        version = version
      )

      val manifest =
        createManifest(location = location, version = version)

      manifest.version shouldBe version
    }

    it("sets a recent createdDate") {
      val manifest = createManifest()

      assertRecent(manifest.createdDate)
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

    it("uses the provided sizes") {
      val paths = Seq("data/file1.txt", "data/file2.txt", "data/dir/file3.txt")
      val expectedSizes = paths.map { _ -> Random.nextLong().abs }.toMap

      val filesWithChecksums =
        paths.map { _ -> randomAlphanumeric }

      val bag = createBagWith(
        manifestFiles = filesWithChecksums.map {
          case (path, checksum) =>
            createBagFileWith(path = path, checksum = checksum)
        }
      )

      val version = createBagVersion
      val location = createPrimaryLocationWith(
        version = version
      )

      val sizes = expectedSizes.map {
        case (path, size) => location.prefix.asLocation(path) -> size
      }

      val sizeFinder = new SizeFinder {
        override def getSize(location: ObjectLocation): Try[Long] =
          Try { sizes(location) }
      }

      val storageManifest = createManifest(
        bag = bag,
        location = location,
        version = version,
        sizeFinder = sizeFinder
      )

      val actualSizes =
        storageManifest.manifest.files
          .map { file =>
            (file.name, file.size)
          }

      actualSizes should contain theSameElementsAs expectedSizes.toSeq
    }

    it("uses the size from the fetch file") {
      val version = createBagVersion
      val bagRoot = createObjectLocation

      val location = createPrimaryLocationWith(
        bagRoot = bagRoot,
        version = version
      )

      val fetchVersion = version.copy(
        underlying = version.underlying - 1
      )

      val fetchLocation = bagRoot.copy(
        path = s"${bagRoot.path}/$fetchVersion/data/file1.txt"
      )

      val bag = createBagWith(
        manifestFiles = Seq(
          createBagFileWith("data/1.txt"),
          createBagFileWith("data/2.txt")
        ),
        fetchEntries = Seq(
          BagFetchEntry(
            uri =
              new URI(s"s3://${fetchLocation.namespace}/${fetchLocation.path}"),
            length = Some(10),
            path = BagPath("data/1.txt")
          ),
          BagFetchEntry(
            uri =
              new URI(s"s3://${fetchLocation.namespace}/${fetchLocation.path}"),
            length = Some(20),
            path = BagPath("data/2.txt")
          )
        )
      )

      val brokenSizeFinder = new SizeFinder {
        override def getSize(location: ObjectLocation): Try[Long] =
          Failure(new Throwable("This should never be called!"))
      }

      val storageManifest = createManifest(
        bag = bag,
        location = location,
        version = version,
        sizeFinder = brokenSizeFinder
      )

      val actualSizes =
        storageManifest.manifest.files
          .map { file =>
            (file.name, file.size)
          }

      actualSizes should contain theSameElementsAs Seq(
        ("data/1.txt", 10L),
        ("data/2.txt", 20L)
      )
    }
  }

  private def createManifest(
    ingestId: IngestID = createIngestID,
    bag: Bag = createBag,
    location: PrimaryStorageLocation = createPrimaryLocationWith(
      version = BagVersion(1)
    ),
    replicas: Seq[SecondaryStorageLocation] = Seq.empty,
    space: StorageSpace = createStorageSpace,
    version: BagVersion = BagVersion(1),
    sizeFinder: SizeFinder = (location: ObjectLocation) =>
      Success(Random.nextLong().abs)
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
