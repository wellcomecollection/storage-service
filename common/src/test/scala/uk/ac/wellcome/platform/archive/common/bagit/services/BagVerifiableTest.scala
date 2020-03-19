package uk.ac.wellcome.platform.archive.common.bagit.services

import java.net.URI

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagFetchMetadata, BagPath}
import uk.ac.wellcome.platform.archive.common.generators.{BagFileGenerators, BagGenerators, FetchMetadataGenerators}
import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, VerifiableLocation}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class BagVerifiableTest
    extends FunSpec
    with Matchers
    with BagGenerators
    with BagFileGenerators
    with FetchMetadataGenerators
    with ObjectLocationGenerators {
  implicit val resolvable: Resolvable[ObjectLocation] =
    (t: ObjectLocation) => new URI(s"example://${t.namespace}/${t.path}")

  val root: ObjectLocation = createObjectLocation
  val bagVerifiable = new BagVerifiable(root)

  describe("creates the correct list of VerifiableLocation") {
    it("for an empty bag") {
      val bag = createBag

      val bagVerifiable = new BagVerifiable(root)

      bagVerifiable.create(bag) shouldBe Right(List.empty)
    }

    it("for a bag that just has manifest files") {
      val manifestEntries = Map(
        BagPath("example.txt") -> createChecksum,
        BagPath("names.txt") -> createChecksum
      )

      val bag = createBagWith(manifestEntries = manifestEntries)

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        getExpectedLocations(manifestEntries)
    }

    it("for a bag that just has tag manifest files") {
      val tagManifestEntries = Map(
        BagPath("tagmanifest-sha256.txt") -> createChecksum,
        BagPath("manifest-sha256.txt") -> createChecksum
      )

      val bag = createBagWith(tagManifestEntries = tagManifestEntries)

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        getExpectedLocations(tagManifestEntries)
    }

    it("for a bag that has both file manifest and tag manifest files") {
      val manifestEntries = Map(
        BagPath("example.txt") -> createChecksum,
        BagPath("names.txt") -> createChecksum
      )

      val tagManifestEntries = Map(
        BagPath("tagmanifest-sha256.txt") -> createChecksum,
        BagPath("manifest-sha256.txt") -> createChecksum
      )

      val bag = createBagWith(
        manifestEntries = manifestEntries,
        tagManifestEntries = tagManifestEntries
      )

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        getExpectedLocations(manifestEntries ++ tagManifestEntries)
    }

    it("for a bag with fetch entries") {
      val manifestEntries = Map(
        BagPath("example.txt") -> createChecksum,
        BagPath("names.txt") -> createChecksum
      )

      val fetchedManifestEntries = Map(
        BagPath("random.txt") -> createChecksum,
        BagPath("cat.jpg") -> createChecksum
      )

      val fetchEntries = fetchedManifestEntries.keys
        .map { _ -> createFetchMetadata }
        .toMap

      val bag = createBagWith(
        manifestEntries = manifestEntries ++ fetchedManifestEntries,
        fetchEntries = fetchEntries
      )

      val expectedLocations =
        getExpectedLocations(manifestEntries) ++
          getExpectedLocations(fetchedManifestEntries, fetchEntries)

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        expectedLocations
    }
  }

  describe("error cases") {
    it("there's a fetch entry for a file that isn't in the bag") {
      val fetchEntries = Map(
        BagPath("example.txt") -> createFetchMetadataWith(
          uri = "s3://example/example.txt"
        )
      )

      val bag = createBagWith(fetchEntries = fetchEntries)

      val result = bagVerifiable.create(bag)
      result shouldBe a[Left[_, _]]
      result.left.get.msg shouldBe "fetch.txt refers to paths that aren't in the bag manifest: example.txt"
    }

    it("there's are multiple fetch entries for a file that isn't in the bag") {
      val fetchEntries = Map(
        BagPath("example.txt") -> createFetchMetadataWith(
          uri = "s3://example/example.txt"
        ),
        BagPath("example.txt") -> createFetchMetadataWith(
          uri = "s3://example/red.txt"
        )
      )

      val bag = createBagWith(fetchEntries = fetchEntries)

      val result = bagVerifiable.create(bag)
      result shouldBe a[Left[_, _]]
      result.left.get.msg shouldBe
        "fetch.txt refers to paths that aren't in the bag manifest: example.txt"
    }
  }

  def createObjectLocationWith(root: ObjectLocation): ObjectLocation =
    root.join(randomAlphanumericWithLength(), randomAlphanumericWithLength())

  def getExpectedLocations(manifestEntries: Map[BagPath, Checksum]): Seq[VerifiableLocation] =
    manifestEntries.map { case (bagPath, checksum) =>
      VerifiableLocation(
        path = bagPath,
        uri = new URI(
          s"example://${root.namespace}/${root.path}/$bagPath"
        ),
        checksum = checksum,
        length = None
      )
    }.toSeq

  def getExpectedLocations(
    manifestEntries: Map[BagPath, Checksum],
    fetchEntries: Map[BagPath, BagFetchMetadata]
  ): Seq[VerifiableLocation] =
    manifestEntries.map { case (bagPath, checksum) =>
      val fetchMetadata = fetchEntries(bagPath)

      VerifiableLocation(
        uri = fetchMetadata.uri,
        path = bagPath,
        checksum = checksum,
        length = fetchMetadata.length
      )
    }.toSeq
}
