package uk.ac.wellcome.platform.archive.indexer.bag

import java.time.Instant

import com.sksamuel.elastic4s.requests.mappings.MappingDefinition
import com.sksamuel.elastic4s.{ElasticClient, Index}
import io.circe.Json
import org.scalatest.Assertion
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.display.bags.DisplayStorageManifest
import uk.ac.wellcome.platform.archive.indexer.IndexerTestCases

import scala.concurrent.ExecutionContext.Implicits.global

class ManifestIndexerTest
    extends IndexerTestCases[StorageManifest, DisplayStorageManifest]
    with StorageManifestGenerators {

  override val mapping: MappingDefinition = ManifestsIndexConfig.mapping

  override def createIndexer(
    client: ElasticClient,
    index: Index
  ): ManifestIndexer =
    new ManifestIndexer(client, index = index)

  override def createDocument: StorageManifest = createStorageManifest

  override def id(storageManifest: StorageManifest): String =
    storageManifest.idWithVersion

  override def assertMatch(
    storedManifest: Map[String, Json],
    manifest: StorageManifest
  ): Assertion = {
    val storedId = storedManifest("id").asString.get
    storedId shouldBe manifest.id.toString

    val fileManifest =
      storedManifest("manifest")
        .as[Map[String, Json]]
        .right
        .get

    val filenames =
      fileManifest("files").asArray.get
        .map { _.as[Map[String, Json]].right.get }
        .map { _("name").asString.get }

    filenames should contain theSameElementsAs manifest.manifest.files.map {
      _.name
    }
  }

  override def createDocumentPair: (StorageManifest, StorageManifest) = {
    val space = createStorageSpace
    val externalIdentifier = createExternalIdentifier
    val version = createBagVersion

    val olderManifest = createStorageManifestWith(
      space = space,
      bagInfo = createBagInfoWith(
        externalIdentifier = externalIdentifier
      ),
      createdDate = Instant.ofEpochMilli(1),
      version = version
    )

    val newerManifest = createStorageManifestWith(
      space = space,
      bagInfo = createBagInfoWith(
        externalIdentifier = externalIdentifier
      ),
      createdDate = Instant.ofEpochMilli(2),
      version = version
    )

    (olderManifest, newerManifest)
  }
}
