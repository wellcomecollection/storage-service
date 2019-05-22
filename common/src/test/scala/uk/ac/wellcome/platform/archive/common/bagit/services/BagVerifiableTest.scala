package uk.ac.wellcome.platform.archive.common.bagit.services

import java.net.URI

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, BagFetch, BagFetchEntry, BagFile, BagManifest, BagPath}
import uk.ac.wellcome.platform.archive.common.generators.BagInfoGenerators
import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, ChecksumValue, SHA256, VerifiableLocation}
import uk.ac.wellcome.storage.ObjectLocation

class BagVerifiableTest extends FunSpec with Matchers with BagInfoGenerators {
  implicit val resolvable: Resolvable[ObjectLocation] =
    (t: ObjectLocation) => new URI(s"example://${t.namespace}/${t.key}")

  val root: ObjectLocation = createObjectLocation
  val bagVerifiable = new BagVerifiable(root)

  describe("creates the correct list of VerifiableLocation") {
    it("for an empty bag") {
      val bag = createBag

      val bagVerifiable = new BagVerifiable(root)

      bagVerifiable.create(bag) shouldBe Right(List.empty)
    }

    it("for a bag that just has manifest files") {
      val manifestFiles = List(
        createBagFileWith("example.txt"),
        createBagFileWith("names.txt")
      )

      val bag = createBagWith(manifestFiles = manifestFiles)

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        getExpectedLocations(manifestFiles)
    }

    it("for a bag that just has tag manifest files") {
      val tagManifestFiles = List(
        createBagFileWith("tag-manifest-sha256.txt"),
        createBagFileWith("manifest-sha256.txt")
      )

      val bag = createBagWith(tagManifestFiles = tagManifestFiles)

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        getExpectedLocations(tagManifestFiles)
    }

    it("for a bag that has both file manifest and tag manifest files") {
      val manifestFiles = List(
        createBagFileWith("example.txt"),
        createBagFileWith("names.txt")
      )

      val tagManifestFiles = List(
        createBagFileWith("tag-manifest-sha256.txt"),
        createBagFileWith("manifest-sha256.txt")
      )

      val bag = createBagWith(
        manifestFiles = manifestFiles,
        tagManifestFiles = tagManifestFiles
      )

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        getExpectedLocations(manifestFiles ++ tagManifestFiles)
    }

    it("for a bag with fetch entries") {
      val manifestFiles = List(
        createBagFileWith("example.txt"),
        createBagFileWith("names.txt")
      )

      val fetchedManifestFiles = List(
        createBagFileWith("random.txt"),
        createBagFileWith("cat.jpg")
      )

      val fetchEntries = fetchedManifestFiles
        .map { bf =>
          createFetchEntryWith(uri = s"s3://example/${bf.path}", path = bf.path)
        }

      val bag = createBagWith(
        manifestFiles = manifestFiles ++ fetchedManifestFiles,
        fetchEntries = fetchEntries
      )

      val expectedLocations =
        getExpectedLocations(manifestFiles) ++
          getExpectedLocations(fetchedManifestFiles, fetchEntries)

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        expectedLocations
    }

    it("for a bag with a repeated (but identical) fetch entry") {
      val manifestFiles = List(
        createBagFileWith("example.txt"),
        createBagFileWith("names.txt")
      )

      val fetchedManifestFiles = List(
        createBagFileWith("random.txt"),
        createBagFileWith("cat.jpg")
      )

      val fetchEntries = fetchedManifestFiles
        .map { bf =>
          createFetchEntryWith(uri = s"s3://example/${bf.path}", path = bf.path)
        }

      val bag = createBagWith(
        manifestFiles = manifestFiles ++ fetchedManifestFiles,
        fetchEntries = fetchEntries ++ fetchEntries
      )

      val expectedLocations =
        getExpectedLocations(manifestFiles) ++
          getExpectedLocations(fetchedManifestFiles, fetchEntries)

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        expectedLocations
    }

    it("for a bag with a repeated (but identical) bag file") {
      val manifestFiles = List(
        createBagFileWith("example.txt"),
        createBagFileWith("names.txt")
      )

      val fetchedManifestFiles = List(
        createBagFileWith("random.txt"),
        createBagFileWith("cat.jpg")
      )

      val fetchEntries = fetchedManifestFiles
        .map { bf =>
          createFetchEntryWith(uri = s"s3://example/${bf.path}", path = bf.path)
        }

      val bag = createBagWith(
        manifestFiles = manifestFiles ++ manifestFiles ++ fetchedManifestFiles,
        fetchEntries = fetchEntries ++ fetchEntries
      )

      val expectedLocations =
        getExpectedLocations(manifestFiles) ++
          getExpectedLocations(fetchedManifestFiles, fetchEntries)

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        expectedLocations
    }
  }

  describe("error cases") {
    it("there's a fetch entry for a file that isn't in the bag") {
      val fetchEntries = Seq(
        createFetchEntryWith(uri = "s3://example/example.txt", path = BagPath("example.txt"))
      )

      val bag = createBagWith(fetchEntries = fetchEntries)

      val result = bagVerifiable.create(bag)
      println(result)
      true shouldBe false
    }

    it("has multiple references to the same file with different checksums") {
      val manifestFiles = List(
        createBagFileWith("example.txt", checksum = "123"),
        createBagFileWith("example.txt", checksum = "456")
      )

      val bag = createBagWith(manifestFiles = manifestFiles)

      val result = bagVerifiable.create(bag)
      println(result)
      true shouldBe false
    }

    it("has multiple, differing fetch entries for the same file") {
      val manifestFiles = List(
        createBagFileWith("example.txt", checksum = "123"),
      )

      val fetchEntries = Seq(
        createFetchEntryWith(uri = "s3://example/example.txt", path = BagPath("example.txt")),
        createFetchEntryWith(uri = "https://example.net/example.txt", path = BagPath("example.txt"))
      )

      val bag = createBagWith(
        manifestFiles = manifestFiles,
        fetchEntries = fetchEntries
      )

      val result = bagVerifiable.create(bag)
      println(result)
      true shouldBe false
    }
  }

  def createBagWith(
    manifestFiles: Seq[BagFile] = List.empty,
    tagManifestFiles: Seq[BagFile] = List.empty,
    fetchEntries: Seq[BagFetchEntry] = List.empty
  ): Bag =
    Bag(
      info = createBagInfo,
      manifest = BagManifest(checksumAlgorithm = SHA256, files = manifestFiles),
      tagManifest = BagManifest(checksumAlgorithm = SHA256, files = tagManifestFiles),
      fetch = if (fetchEntries.isEmpty) None else Some(BagFetch(fetchEntries))
    )

  def createBag: Bag = createBagWith()

  def createBagFileWith(
    path: String,
    checksum: String = "abc",
  ): BagFile =
    BagFile(
      checksum = Checksum(
        algorithm = SHA256,
        value = ChecksumValue(checksum)
      ),
      path = BagPath(path)
    )

  def createObjectLocation: ObjectLocation =
    ObjectLocation(
      namespace = randomAlphanumeric(),
      key = randomAlphanumeric()
    )

  def createObjectLocationWith(root: ObjectLocation): ObjectLocation =
    root.join(randomAlphanumeric(), randomAlphanumeric())

  def createFetchEntryWith(
    uri: String,
    path: BagPath
  ): BagFetchEntry =
    BagFetchEntry(
      uri = new URI(uri),
      length = None,
      path = path
    )

  def getExpectedLocations(bagFiles: Seq[BagFile]): Seq[VerifiableLocation] =
    bagFiles.map { mf =>
      VerifiableLocation(
        uri = new URI(s"example://${root.namespace}/${root.key}/${mf.path.toString}"),
        checksum = mf.checksum
      )
    }

  def getExpectedLocations(bagFiles: Seq[BagFile], fetchEntries: Seq[BagFetchEntry]): Seq[VerifiableLocation] =
    bagFiles.zip(fetchEntries).map { case (bagFile, fetchEntry) =>
      VerifiableLocation(
        uri = fetchEntry.uri,
        checksum = bagFile.checksum
      )
    }
}
