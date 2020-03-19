package uk.ac.wellcome.platform.archive.common.bagit.services

import java.net.URI

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagFetchMetadata, BagPath}
import uk.ac.wellcome.platform.archive.common.generators.{BagFileGenerators, BagGenerators, FetchMetadataGenerators}
import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, ChecksumValue, HashingAlgorithm, VerifiableLocation}
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
        BagPath("example.txt") -> randomChecksumValue,
        BagPath("names.txt") -> randomChecksumValue
      )

      val manifestChecksumAlgorithm = randomHashingAlgorithm

      val bag = createBagWith(
        manifestEntries = manifestEntries,
        manifestChecksumAlgorithm = manifestChecksumAlgorithm
      )

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        getExpectedLocations(
          manifestEntries = manifestEntries,
          checksumAlgorithm = manifestChecksumAlgorithm
        )
    }

    it("for a bag that just has tag manifest files") {
      val tagManifestEntries = Map(
        BagPath("tagmanifest-sha256.txt") -> randomChecksumValue,
        BagPath("manifest-sha256.txt") -> randomChecksumValue
      )

      val tagManifestChecksumAlgorithm = randomHashingAlgorithm

      val bag = createBagWith(
        tagManifestEntries = tagManifestEntries,
        tagManifestChecksumAlgorithm = tagManifestChecksumAlgorithm
      )

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        getExpectedLocations(
          manifestEntries = tagManifestEntries,
          checksumAlgorithm = tagManifestChecksumAlgorithm
        )
    }

    it("for a bag that has both file manifest and tag manifest files") {
      val manifestEntries = Map(
        BagPath("example.txt") -> randomChecksumValue,
        BagPath("names.txt") -> randomChecksumValue
      )

      val tagManifestEntries = Map(
        BagPath("tagmanifest-sha256.txt") -> randomChecksumValue,
        BagPath("manifest-sha256.txt") -> randomChecksumValue
      )

      val checksumAlgorithm = randomHashingAlgorithm

      val bag = createBagWith(
        manifestEntries = manifestEntries,
        manifestChecksumAlgorithm = checksumAlgorithm,
        tagManifestEntries = tagManifestEntries,
        tagManifestChecksumAlgorithm = checksumAlgorithm
      )

      bagVerifiable.create(bag).right.get should contain theSameElementsAs
        getExpectedLocations(
          manifestEntries = manifestEntries ++ tagManifestEntries,
          checksumAlgorithm = checksumAlgorithm
        )
    }

    it("for a bag with fetch entries") {
      val manifestEntries = Map(
        BagPath("example.txt") -> randomChecksumValue,
        BagPath("names.txt") -> randomChecksumValue
      )

      val fetchedManifestEntries = Map(
        BagPath("random.txt") -> randomChecksumValue,
        BagPath("cat.jpg") -> randomChecksumValue
      )

      val fetchEntries = fetchedManifestEntries.keys
        .map { _ -> createFetchMetadata }
        .toMap

      val manifestChecksumAlgorithm = randomHashingAlgorithm

      val bag = createBagWith(
        manifestEntries = manifestEntries ++ fetchedManifestEntries,
        manifestChecksumAlgorithm = manifestChecksumAlgorithm,
        fetchEntries = fetchEntries
      )

      val expectedLocations =
        getExpectedLocations(
          manifestEntries = manifestEntries,
          checksumAlgorithm = manifestChecksumAlgorithm
        ) ++
          getExpectedLocations(
            manifestEntries = fetchedManifestEntries,
            checksumAlgorithm = manifestChecksumAlgorithm,
            fetchEntries = fetchEntries
          )

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

  def getExpectedLocations(
    manifestEntries: Map[BagPath, ChecksumValue],
    checksumAlgorithm: HashingAlgorithm): Seq[VerifiableLocation] =
    manifestEntries.map { case (bagPath, checksumValue) =>
      VerifiableLocation(
        path = bagPath,
        uri = new URI(
          s"example://${root.namespace}/${root.path}/$bagPath"
        ),
        checksum = Checksum(
          algorithm = checksumAlgorithm,
          value = checksumValue
        ),
        length = None
      )
    }.toSeq

  def getExpectedLocations(
    manifestEntries: Map[BagPath, ChecksumValue],
    checksumAlgorithm: HashingAlgorithm,
    fetchEntries: Map[BagPath, BagFetchMetadata]
  ): Seq[VerifiableLocation] =
    manifestEntries.map { case (bagPath, checksumValue) =>
      val fetchMetadata = fetchEntries(bagPath)

      VerifiableLocation(
        uri = fetchMetadata.uri,
        path = bagPath,
        checksum = Checksum(
          algorithm = checksumAlgorithm,
          value = checksumValue
        ),
        length = fetchMetadata.length
      )
    }.toSeq
}
