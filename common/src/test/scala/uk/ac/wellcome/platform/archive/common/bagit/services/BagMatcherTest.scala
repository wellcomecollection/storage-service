package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
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
          fetchEntries = Seq.empty
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
        fetchEntries = Seq.empty
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

      val fetchEntry = createFetchEntry
      val fetchBagFile = createBagFileWith(
        path = fetchEntry.path.value
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = bagFiles :+ fetchBagFile,
        fetchEntries = Seq(fetchEntry)
      )

      val expectedLocations = bagFiles.map { bagFile =>
        MatchedLocation(bagFile, fetchEntry = None)
      } :+ MatchedLocation(fetchBagFile, fetchEntry = Some(fetchEntry))

      result.right.value should contain theSameElementsAs expectedLocations
    }

    it("for a bag with a repeated (but identical) fetch entry") {
      val fetchEntry = createFetchEntry
      val bagFile = createBagFileWith(
        path = fetchEntry.path.value
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = Seq(bagFile),
        fetchEntries = Seq(fetchEntry, fetchEntry)
      )

      result.right.value shouldBe Seq(
        MatchedLocation(bagFile, fetchEntry = Some(fetchEntry))
      )
    }

    it("for a bag with a repeated (but identical) bag file") {
      val fetchEntry = createFetchEntry
      val bagFile = createBagFileWith(
        path = fetchEntry.path.value
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = Seq(bagFile, bagFile),
        fetchEntries = Seq(fetchEntry)
      )

      result.right.value shouldBe Seq(
        MatchedLocation(bagFile, fetchEntry = Some(fetchEntry))
      )
    }
  }

  describe("error cases") {
    it("there's a fetch entry for a file that isn't in the bag") {
      val fetchEntries = Seq(createFetchEntry)

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = Seq.empty,
        fetchEntries = fetchEntries
      )

      result.left.value.head.getMessage shouldBe
        s"Fetch entry refers to a path that isn't in the bag manifest: ${fetchEntries.head.path}"
    }

    it("there are multiple fetch entries for files that aren't in the bag") {
      val bagPath = BagPath(randomAlphanumeric)
      val fetchEntries = (1 to 3).map { _ =>
        createFetchEntryWith(path = bagPath)
      }

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = Seq.empty,
        fetchEntries = fetchEntries
      )

      result.left.value.head.getMessage shouldBe
        s"Fetch entry refers to a path that isn't in the bag manifest: $bagPath"
    }

    it("has multiple, differing fetch entries for the same file") {
      val bagFile = createBagFile

      val fetchEntries = (1 to 3).map { _ =>
        createFetchEntryWith(
          path = bagFile.path
        )
      }

      val result = BagMatcher.correlateFetchEntryToBagFile(
        bagFiles = Seq(bagFile),
        fetchEntries = fetchEntries
      )

      result.left.value.head.getMessage should startWith(
        "Multiple, ambiguous entries for the same path"
      )
    }
  }
}
