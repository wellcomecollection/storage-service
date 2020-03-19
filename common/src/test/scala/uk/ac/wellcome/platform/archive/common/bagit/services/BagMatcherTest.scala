package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagPath,
  MatchedLocation,
  PayloadManifest
}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.generators.FetchMetadataGenerators
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, MD5, SHA256}

class BagMatcherTest
    extends FunSpec
    with Matchers
    with EitherValues
    with FetchMetadataGenerators
    with StorageRandomThings {

  describe("creates the correct list of MatchedLocations") {
    it("for an empty bag") {
      BagMatcher
        .correlateFetchEntryToBagFile(
          manifest = PayloadManifest(
            checksumAlgorithm = SHA256,
            entries = Map.empty
          ),
          fetchEntries = Map.empty
        )
        .right
        .value shouldBe Seq.empty
    }

    it("for a bag that doesn't have any fetch entries") {
      val manifestEntries = Map(
        createBagPath -> randomChecksumValue,
        createBagPath -> randomChecksumValue,
        createBagPath -> randomChecksumValue,
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifest = PayloadManifest(
          checksumAlgorithm = SHA256,
          entries = manifestEntries
        ),
        fetchEntries = Map.empty
      )

      result.right.value shouldBe manifestEntries.map { case (bagPath, checksumValue) =>
        MatchedLocation(
          bagPath = bagPath,
          checksum = Checksum(
            algorithm = SHA256,
            value = checksumValue
          ),
          fetchMetadata = None
        )
      }
    }

    it("uses the hashing algorithm from the manifest") {
      val manifestEntries = Map(
        createBagPath -> randomChecksumValue,
        createBagPath -> randomChecksumValue,
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifest = PayloadManifest(
          checksumAlgorithm = MD5,
          entries = manifestEntries
        ),
        fetchEntries = Map.empty
      )

      result.right.value shouldBe manifestEntries.map { case (bagPath, checksumValue) =>
        MatchedLocation(
          bagPath = bagPath,
          checksum = Checksum(
            algorithm = MD5,
            value = checksumValue
          ),
          fetchMetadata = None
        )
      }
    }

    it("for a bag with fetch entries") {
      val manifestEntries = Map(
        createBagPath -> randomChecksumValue,
        createBagPath -> randomChecksumValue,
        createBagPath -> randomChecksumValue,
      )

      val fetchMetadata = createFetchMetadata
      val fetchPath = BagPath(randomAlphanumeric)
      val fetchChecksumValue = randomChecksumValue

      val checksumAlgorithm = randomHashingAlgorithm

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifest = PayloadManifest(
          checksumAlgorithm = checksumAlgorithm,
          entries = manifestEntries ++ Map(fetchPath -> fetchChecksumValue)
        ),
        fetchEntries = Map(fetchPath -> fetchMetadata)
      )

      val expectedLocations = manifestEntries.map { case (bagPath, checksumValue) =>
        MatchedLocation(
          bagPath = bagPath,
          checksum = Checksum(
            algorithm = checksumAlgorithm,
            value = checksumValue
          ),
          fetchMetadata = None
        )
      }.toSeq :+ MatchedLocation(
        bagPath = fetchPath,
        checksum = Checksum(
          algorithm = checksumAlgorithm,
          value = fetchChecksumValue
        ),
        fetchMetadata = Some(fetchMetadata)
      )

      result.right.value should contain theSameElementsAs expectedLocations
    }
  }

  describe("error cases") {
    it("there's a fetch entry for a file that isn't in the bag") {
      val fetchPath = BagPath(randomAlphanumeric)
      val fetchEntries = Map(fetchPath -> createFetchMetadata)

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifest = PayloadManifest(
          checksumAlgorithm = SHA256,
          entries = Map.empty
        ),
        fetchEntries = fetchEntries
      )

      result.left.value.getMessage shouldBe
        s"fetch.txt refers to paths that aren't in the bag manifest: $fetchPath"
    }

    it("there are multiple fetch entries for files that aren't in the bag") {
      val fetchEntries = (1 to 3).map { _ =>
        BagPath(randomAlphanumeric) -> createFetchMetadata
      }.toMap

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifest = PayloadManifest(
          checksumAlgorithm = SHA256,
          entries = Map.empty
        ),
        fetchEntries = fetchEntries
      )

      result.left.value.getMessage should startWith(
        s"fetch.txt refers to paths that aren't in the bag manifest:"
      )
    }
  }
}
