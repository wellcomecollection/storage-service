package weco.storage_service.indexer.bags.models

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.generators.{
  IngestGenerators,
  StorageManifestGenerators
}
import weco.storage_service.storage.services.DestinationBuilder

class IndexedStorageManifestTest
    extends AnyFunSpec
    with Matchers
    with IngestGenerators
    with StorageManifestGenerators {

  it("correctly assigns payload stats") {
    val v3Version: BagVersion = BagVersion(3)
    val ingest = createIngestWith(version = Some(v3Version))

    val v1PathPrefix = DestinationBuilder
      .buildPath(
        ingest.space,
        ingest.externalIdentifier,
        BagVersion(1)
      )

    val v2pathPrefix = DestinationBuilder
      .buildPath(
        ingest.space,
        ingest.externalIdentifier,
        BagVersion(2)
      )

    val v3pathPrefix = DestinationBuilder
      .buildPath(
        ingest.space,
        ingest.externalIdentifier,
        v3Version
      )

    val v1OneFileSize = 123L
    val v1OneFileName = "v1_1.gif"
    val v1Files = List(
      createStorageManifestFileWith(
        pathPrefix = v1PathPrefix,
        name = v1OneFileName,
        size = v1OneFileSize
      )
    )

    val v2OneFileSize = 321L
    val v2OneFileName = "v2_1.GIF"

    val v2TwoFileSize = 456L
    val v2TwoFileName = "v2_2.txt"

    val v2Files = List(
      createStorageManifestFileWith(
        pathPrefix = v2pathPrefix,
        name = v2OneFileName,
        size = v2OneFileSize
      ),
      createStorageManifestFileWith(
        pathPrefix = v2pathPrefix,
        name = v2TwoFileName,
        size = v2TwoFileSize
      )
    )

    val v3OneFileSize = 654L
    val v3OneFileName = "v3_1.png"

    val v3Files = List(
      createStorageManifestFileWith(
        pathPrefix = v3pathPrefix,
        name = v3OneFileName,
        size = v3OneFileSize
      )
    )

    val files = v1Files ++ v2Files ++ v3Files

    val storageManifest = createStorageManifestWith(
      ingestId = ingest.id,
      space = ingest.space,
      externalIdentifier = ingest.externalIdentifier,
      version = v3Version,
      files = files
    )

    val indexedManifest = IndexedStorageManifest(storageManifest)

    val expectedFileCount = v1Files.length + v2Files.length + v3Files.length
    val expectedFilesTotalSize = v1OneFileSize + v2OneFileSize + v2TwoFileSize + v3OneFileSize

    indexedManifest.id shouldBe storageManifest.id.toString
    indexedManifest.space shouldBe storageManifest.space.toString
    indexedManifest.filesCount shouldBe expectedFileCount
    indexedManifest.filesTotalSize shouldBe expectedFilesTotalSize
  }
}
