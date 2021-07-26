package weco.storage_service.bagit.services

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.bagit.models.{MatchedLocation, NewPayloadManifest}
import weco.storage_service.generators.{
  FetchMetadataGenerators,
  StorageRandomGenerators
}
import weco.storage_service.checksum.{
  Checksum,
  MD5,
  MultiManifestChecksum,
  SHA256
}

class BagMatcherTest
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with FetchMetadataGenerators
    with StorageRandomGenerators {

  def randomMultiChecksum: MultiManifestChecksum =
    MultiManifestChecksum(
      md5 = Some(randomChecksumValue),
      sha1 = None,
      sha256 = Some(randomChecksumValue),
      sha512 = None
    )

  describe("creates the correct list of MatchedLocations") {
    it("for an empty bag") {
      BagMatcher
        .correlateFetchEntryToBagFile(
          manifest = NewPayloadManifest(
            entries = Map.empty,
            algorithms = Set(SHA256)
          ),
          algorithm = SHA256,
          fetchEntries = Map.empty
        )
        .value shouldBe Seq.empty
    }

    it("for a bag that doesn't have any fetch entries") {
      val manifestEntries = Map(
        createBagPath -> randomMultiChecksum,
        createBagPath -> randomMultiChecksum,
        createBagPath -> randomMultiChecksum
      )

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifest = NewPayloadManifest(
          entries = manifestEntries,
          algorithms = Set(MD5, SHA256)
        ),
        algorithm = SHA256,
        fetchEntries = Map.empty
      )

      result.value shouldBe manifestEntries.map {
        case (bagPath, multiChecksum) =>
          MatchedLocation(
            bagPath = bagPath,
            multiChecksum = multiChecksum,
            algorithm = SHA256,
            fetchMetadata = None
          )
      }

      result.value.foreach { loc =>
        loc.checksum shouldBe Checksum(
          algorithm = SHA256,
          value = loc.multiChecksum.sha256.get
        )
      }
    }

    it("for a bag with fetch entries") {
      val manifestEntries = Map(
        createBagPath -> randomMultiChecksum,
        createBagPath -> randomMultiChecksum,
        createBagPath -> randomMultiChecksum
      )

      val fetchMetadata = createFetchMetadata
      val fetchPath = createBagPath
      val fetchMultiChecksum = randomMultiChecksum

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifest = NewPayloadManifest(
          entries = manifestEntries ++ Map(fetchPath -> fetchMultiChecksum),
          algorithms = Set(MD5, SHA256)
        ),
        algorithm = SHA256,
        fetchEntries = Map(fetchPath -> fetchMetadata)
      )

      val expectedLocations = manifestEntries.map {
        case (bagPath, multiChecksum) =>
          MatchedLocation(
            bagPath = bagPath,
            multiChecksum = multiChecksum,
            algorithm = SHA256,
            fetchMetadata = None
          )
      }.toSeq :+ MatchedLocation(
        bagPath = fetchPath,
        multiChecksum = fetchMultiChecksum,
        algorithm = SHA256,
        fetchMetadata = Some(fetchMetadata)
      )

      result.value should contain theSameElementsAs expectedLocations
    }
  }

  describe("error cases") {
    it("there's a fetch entry for a file that isn't in the bag") {
      val fetchPath = createBagPath
      val fetchEntries = Map(fetchPath -> createFetchMetadata)

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifest = NewPayloadManifest(
          entries = Map.empty,
          algorithms = Set(SHA256)
        ),
        algorithm = SHA256,
        fetchEntries = fetchEntries
      )

      result.left.value.getMessage shouldBe
        s"fetch.txt refers to paths that aren't in the bag manifest: $fetchPath"
    }

    it("there are multiple fetch entries for files that aren't in the bag") {
      val fetchEntries = (1 to 3).map { _ =>
        createBagPath -> createFetchMetadata
      }.toMap

      val result = BagMatcher.correlateFetchEntryToBagFile(
        manifest = NewPayloadManifest(
          entries = Map.empty,
          algorithms = Set(SHA256)
        ),
        algorithm = SHA256,
        fetchEntries = fetchEntries
      )

      result.left.value.getMessage should startWith(
        s"fetch.txt refers to paths that aren't in the bag manifest:"
      )
    }
  }
}
