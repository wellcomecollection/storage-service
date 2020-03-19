package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagFile,
  BagPath,
  MatchedLocation
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagFileGenerators,
  FetchMetadataGenerators
}

class BagMatcherTest
    extends FunSpec
    with Matchers
    with EitherValues
    with FetchMetadataGenerators
    with BagFileGenerators {

  describe("creates the correct list of MatchedLocations") {
    it("for an empty bag") {
      BagMatcher
        .correlateFetchEntryToBagFile(
          manifestEntries = Map.empty,
          fetchEntries = Map.empty
        )
        .right
        .value shouldBe Seq.empty
    }

    it("for a bag that doesn't have any fetch entries") {
      val manifestEntries = Map(
        createBagPath -> createChecksum,
        createBagPath -> createChecksum,
        createBagPath -> createChecksum,
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifestEntries = manifestEntries,
        fetchEntries = Map.empty
      )

      result.right.value shouldBe manifestEntries.map { case (path, checksum) =>
        MatchedLocation(bagFile = BagFile(checksum = checksum, path = path), fetchMetadata = None)
      }
    }

    it("for a bag with fetch entries") {
      val manifestEntries = Map(
        createBagPath -> createChecksum,
        createBagPath -> createChecksum,
        createBagPath -> createChecksum,
      )

      val fetchMetadata = createFetchMetadata
      val fetchPath = BagPath(randomAlphanumeric)

      val fetchBagFile = createBagFileWith(
        path = fetchPath.value
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifestEntries = manifestEntries ++ Map(fetchPath -> fetchBagFile.checksum),
        fetchEntries = Map(fetchPath -> fetchMetadata)
      )

      val expectedLocations = manifestEntries.map { case (path, checksum) =>
        MatchedLocation(bagFile = BagFile(checksum = checksum, path = path), fetchMetadata = None)
      }.toSeq :+ MatchedLocation(fetchBagFile, fetchMetadata = Some(fetchMetadata))

      result.right.value should contain theSameElementsAs expectedLocations
    }
  }

  describe("error cases") {
    it("there's a fetch entry for a file that isn't in the bag") {
      val fetchPath = BagPath(randomAlphanumeric)
      val fetchEntries = Map(fetchPath -> createFetchMetadata)

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifestEntries = Map.empty,
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
        manifestEntries = Map.empty,
        fetchEntries = fetchEntries
      )

      result.left.value.getMessage should startWith(
        s"fetch.txt refers to paths that aren't in the bag manifest:"
      )
    }
  }
}
