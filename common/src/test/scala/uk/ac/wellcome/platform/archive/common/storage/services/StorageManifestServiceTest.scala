package uk.ac.wellcome.platform.archive.common.storage.services

import java.net.URI

import org.scalatest.{Assertion, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagFetchEntry,
  BagPath,
  BagVersion
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagFileGenerators,
  BagGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageManifest,
  StorageSpace
}
import uk.ac.wellcome.platform.archive.common.verify.{MD5, SHA256}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

import scala.util.{Failure, Random, Success, Try}

class StorageManifestServiceTest
    extends FunSpec
    with Matchers
    with BagGenerators
    with BagFileGenerators
    with ObjectLocationGenerators
    with StorageSpaceGenerators
    with TimeTestFixture
    with TryValues {

  it("fails if the replica root is not a versioned directory") {
    val replicaRootLocation = createObjectLocation
    val version = randomInt(1, 10)

    assertIsError(replicaRoot = replicaRootLocation, version = version) { err =>
      err shouldBe a[StorageManifestException]
      err.getMessage shouldBe s"Malformed bag root: $replicaRootLocation (expected suffix /v$version)"
    }
  }

  it("fails if the replica root has the wrong version") {
    val version = randomInt(1, 10)
    val replicaRootLocation = createObjectLocation.join(s"/v${version + 1}")

    assertIsError(replicaRoot = replicaRootLocation, version = version) { err =>
      err shouldBe a[StorageManifestException]
      err.getMessage shouldBe s"Malformed bag root: $replicaRootLocation (expected suffix /v$version)"
    }
  }

  it("identifies the correct root location of a bag") {
    val version = randomInt(1, 10)
    val bagRoot = createObjectLocation
    val replicaRoot = bagRoot.join(s"/v$version")

    val storageManifest = createManifest(
      replicaRoot = replicaRoot,
      version = version
    )

    storageManifest.locations shouldBe Seq(
      StorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = bagRoot
      )
    )
  }

  describe("constructs the paths correctly") {
    it(
      "if there are no fetch entries, it puts all the file entries under a versioned path"
    ) {
      val version = randomInt(1, 10)
      val replicaRoot = createObjectLocation.join(s"/v$version")

      val files = Seq("data/file1.txt", "data/file2.txt", "data/dir/file3.txt")

      val bag = createBagWith(
        manifestFiles = files.map { path =>
          createBagFileWith(path = path)
        }
      )

      val storageManifest = createManifest(
        bag = bag,
        replicaRoot = replicaRoot,
        version = version,
        sizes = files.map { replicaRoot.join(_) -> Random.nextLong().abs }.toMap
      )

      val namePathMap =
        storageManifest.manifest.files.map { file =>
          (file.name, file.path)
        }.toMap

      namePathMap shouldBe files.map { f =>
        (f, s"v$version/$f")
      }.toMap
    }

    it(
      "if there are no fetch entries, it puts all the tag manifest under a versioned path"
    ) {
      val version = randomInt(1, 10)
      val replicaRoot = createObjectLocation.join(s"/v$version")

      val files =
        Seq("bag-info.txt", "tag-manifest-sha256.txt", "manifest-sha256.txt")

      val bag = createBagWith(
        tagManifestFiles = files.map { path =>
          createBagFileWith(path = path)
        }
      )

      val storageManifest = createManifest(
        bag = bag,
        replicaRoot = replicaRoot,
        version = version,
        sizes = files.map { replicaRoot.join(_) -> Random.nextLong().abs }.toMap
      )

      val namePathMap =
        storageManifest.tagManifest.files.map { file =>
          (file.name, file.path)
        }.toMap

      namePathMap shouldBe files.map { f =>
        (f, s"v$version/$f")
      }.toMap
    }

    it("puts a fetch entry under the right versioned path") {
      val version = randomInt(1, 10)
      val fetchVersion = version - 1
      val bagRoot = createObjectLocation
      val replicaRoot = bagRoot.join(s"/v$version")

      val fetchLocation = bagRoot.copy(
        path = s"${bagRoot.path}/v$fetchVersion/data/file1.txt"
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

      val files = Seq(
        fetchLocation,
        bagRoot.join(s"v$version/data/file1.txt")
      )

      val storageManifest = createManifest(
        bag = bag,
        replicaRoot = replicaRoot,
        version = version,
        sizes = files.map { _ -> Random.nextLong().abs }.toMap
      )

      storageManifest.manifest.files.map { f =>
        f.name -> f.path
      }.toMap shouldBe Map("data/file1.txt" -> s"v$fetchVersion/data/file1.txt")
    }

    it("uses a mixture of fetch entries and concrete files") {
      val version = randomInt(1, 10)
      val fetchVersion = version - 1
      val bagRoot = createObjectLocation
      val replicaRoot = bagRoot.join(s"/v$version")

      val fetchLocation = bagRoot.copy(
        path = s"${bagRoot.path}/v$fetchVersion/data/file1.txt"
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

      val files = Seq(
        fetchLocation,
        bagRoot.join(s"v$version/data/file1.txt"),
        bagRoot.join(s"v$version/data/file2.txt")
      )

      val storageManifest = createManifest(
        bag = bag,
        replicaRoot = replicaRoot,
        version = version,
        sizes = files.map { _ -> Random.nextLong().abs }.toMap
      )

      storageManifest.manifest.files.map { f =>
        f.name -> f.path
      }.toMap shouldBe Map(
        "data/file1.txt" -> s"v$fetchVersion/data/file1.txt",
        "data/file2.txt" -> s"v$version/data/file2.txt"
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

      val version = randomInt(from = 1, to = 10)
      val replicaRoot = createObjectLocation.join(s"v$version")
      val sizes = paths.map { replicaRoot.join(_) -> Random.nextLong() }.toMap

      val storageManifest = createManifest(
        bag = bag,
        replicaRoot = replicaRoot,
        version = version,
        sizes = sizes
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

      val version = randomInt(from = 1, to = 10)
      val replicaRoot = createObjectLocation.join(s"v$version")
      val sizes = paths.map { replicaRoot.join(_) -> Random.nextLong() }.toMap

      val storageManifest = createManifest(
        bag = bag,
        replicaRoot = replicaRoot,
        version = version,
        sizes = sizes
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
      val version = randomInt(1, 10)
      val bagRoot = createObjectLocation
      val replicaRoot = bagRoot.join(s"/v$version")

      val fetchEntries = Seq(
        BagFetchEntry(
          uri = new URI(s"s3://${replicaRoot.namespace}/file1.txt"),
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

      assertIsError(bag = bag, replicaRoot = replicaRoot, version = version) {
        err =>
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
      val version = randomInt(1, 10)
      val bagRoot = createObjectLocation
      val replicaRoot = bagRoot.join(s"/v$version")

      val manifest =
        createManifest(replicaRoot = replicaRoot, version = version)

      manifest.version shouldBe BagVersion(version)
    }

    it("sets a recent createdDate") {
      val manifest = createManifest()

      assertRecent(manifest.createdDate)
    }
  }

  describe("adds the size to the manifest files") {
    it("fails if it cannot get the size of a file") {
      val version = randomInt(1, 10)
      val bagRoot = createObjectLocation.join(s"/v$version")

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
        bag = bag,
        replicaRoot = bagRoot.asPrefix,
        space = createStorageSpace,
        version = BagVersion(version)
      )

      result.failed.get shouldBe a[StorageManifestException]
      result.failed.get.getMessage should startWith(
        s"Error getting size of ${bagRoot.join("data/file1.txt")}"
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

      val version = randomInt(from = 1, to = 10)
      val replicaRoot = createObjectLocation.join(s"v$version")
      val sizes = expectedSizes.map {
        case (path, size) => replicaRoot.join(path) -> size
      }

      val storageManifest = createManifest(
        bag = bag,
        replicaRoot = replicaRoot,
        version = version,
        sizes = sizes
      )

      val actualSizes =
        storageManifest.manifest.files
          .map { file =>
            (file.name, file.size)
          }

      actualSizes should contain theSameElementsAs expectedSizes.toSeq
    }

    it("uses the size from the fetch file") {
      val version = randomInt(1, 10)
      val fetchVersion = version - 1
      val bagRoot = createObjectLocation
      val replicaRoot = bagRoot.join(s"/v$version")

      val fetchLocation = bagRoot.copy(
        path = s"${bagRoot.path}/v$fetchVersion/data/file1.txt"
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

      val service = new StorageManifestService(brokenSizeFinder)

      val storageManifest = service
        .createManifest(
          bag = bag,
          replicaRoot = replicaRoot.asPrefix,
          space = createStorageSpace,
          version = BagVersion(version)
        )
        .success
        .value

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
    space: StorageSpace = createStorageSpace,
    bag: Bag = createBag,
    replicaRoot: ObjectLocation = createObjectLocation.join("/v1"),
    version: Int = 1,
    sizes: Map[ObjectLocation, Long] = Map.empty
  ): StorageManifest = {
    val sizeFinder = new SizeFinder {
      override def getSize(location: ObjectLocation): Try[Long] = Try {
        sizes.getOrElse(
          location,
          throw new Throwable(s"No such size for location $location!")
        )
      }
    }

    val service = new StorageManifestService(sizeFinder)

    val result = service.createManifest(
      bag = bag,
      replicaRoot = replicaRoot.asPrefix,
      space = space,
      version = BagVersion(version)
    )

    if (result.isFailure) {
      println(result)
    }

    result.success.value
  }

  private def assertIsError(
    bag: Bag = createBag,
    replicaRoot: ObjectLocation = createObjectLocation.join("/v1"),
    version: Int = 1
  )(assertError: Throwable => Assertion): Assertion = {
    val sizeFinder = new SizeFinder {
      override def getSize(location: ObjectLocation): Try[Long] = Success(1)
    }

    val service = new StorageManifestService(sizeFinder)

    val result = service.createManifest(
      bag = bag,
      replicaRoot = replicaRoot.asPrefix,
      space = createStorageSpace,
      version = BagVersion(version)
    )

    assertError(result.failure.exception)
  }
}
