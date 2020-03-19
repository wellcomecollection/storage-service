package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagFetchEntry,
  BagPath,
  MatchedLocation
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagFileGenerators,
  FetchEntryGenerators
}

class BagMatcherTest
    extends FunSpec
    with Matchers
    with EitherValues
    with FetchEntryGenerators
    with BagFileGenerators {

  describe("creates the correct list of MatchedLocations") {
    it("for an empty bag") {
      BagMatcher
        .correlateFetchEntryToBagFile(
          bagFiles = Seq.empty,
          fetchEntries = Map.empty
        )
        .right
        .value shouldBe Seq.empty
    }

    it("for a bag that doesn't have any fetch entries") {
      val bagFiles = Seq(
        createBagFile,
        createBagFile,
        createBagFile
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = bagFiles,
        fetchEntries = Map.empty
      )

      result.right.value shouldBe bagFiles.map { bagFile =>
        MatchedLocation(bagFile, fetchEntry = None)
      }
    }

    it("for a bag with fetch entries") {
      val bagFiles = Seq(
        createBagFile,
        createBagFile,
        createBagFile
      )

      val fetchMetadata = createFetchMetadata
      val fetchPath = BagPath(randomAlphanumeric)

      val fetchEntry = BagFetchEntry(
        uri = fetchMetadata.uri,
        length = fetchMetadata.length,
        path = fetchPath
      )
      val fetchBagFile = createBagFileWith(
        path = fetchPath.value
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = bagFiles :+ fetchBagFile,
        fetchEntries = Map(fetchPath -> fetchMetadata)
      )

      val expectedLocations = bagFiles.map { bagFile =>
        MatchedLocation(bagFile, fetchEntry = None)
      } :+ MatchedLocation(fetchBagFile, fetchEntry = Some(fetchEntry))

      result.right.value should contain theSameElementsAs expectedLocations
    }

    it("for a bag with a repeated (but identical) bag file") {
      val fetchMetadata = createFetchMetadata
      val fetchPath = BagPath(randomAlphanumeric)
      val fetchEntry = BagFetchEntry(
        uri = fetchMetadata.uri,
        length = fetchMetadata.length,
        path = fetchPath
      )

      val bagFile = createBagFileWith(
        path = fetchPath.value
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = Seq(bagFile, bagFile),
        fetchEntries = Map(fetchPath -> fetchMetadata)
      )

      result.right.value shouldBe Seq(
        MatchedLocation(bagFile, fetchEntry = Some(fetchEntry))
      )
    }
  }

  describe("error cases") {
    it("there's a fetch entry for a file that isn't in the bag") {
      val fetchPath = BagPath(randomAlphanumeric)
      val fetchEntries = Map(fetchPath -> createFetchMetadata)

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = Seq.empty,
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
        bagFiles = Seq.empty,
        fetchEntries = fetchEntries
      )

      result.left.value.getMessage should startWith(
        s"fetch.txt refers to paths that aren't in the bag manifest:"
      )
    }
  }
}
