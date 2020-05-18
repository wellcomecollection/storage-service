package uk.ac.wellcome.platform.archive.indexer.bag

import com.sksamuel.elastic4s.Index
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.indexer.bags.{BagIndexer, BagsIndexConfig}
import uk.ac.wellcome.platform.archive.indexer.bags.models.IndexedStorageManifest
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.Indexer
import uk.ac.wellcome.platform.archive.indexer.fixtures.IndexerFixtures
import uk.ac.wellcome.json.JsonUtil._

import scala.concurrent.ExecutionContext.Implicits.global


class BagIndexerFeatureTest
  extends AnyFunSpec
  with Matchers
  with EitherValues
  with IndexerFixtures[StorageManifest, IndexedStorageManifest]
  with StorageManifestGenerators {

    def convertIndexedT(t: StorageManifest): IndexedStorageManifest = IndexedStorageManifest(t)
    def createT: (StorageManifest, String) = {
      val storageManifest = createStorageManifest

      (storageManifest, storageManifest.id.toString)
    }

    override def createIndexer(index: Index): Indexer[StorageManifest, IndexedStorageManifest] =
      new BagIndexer(
        client = elasticClient,
        index = index
      )

    it("processes a single message") {
      withLocalElasticsearchIndex(BagsIndexConfig.mapping) { index =>
        withLocalSqsQueue() { queue =>
          withIndexerWorker(index, queue) { worker =>
            val (t, id) = createT

            worker.run()

            sendNotificationToSQS(queue, t)

            eventually {
              val storedIndexedT = getT[IndexedStorageManifest](index, id)

             storedIndexedT shouldBe convertIndexedT(t)
            }
          }
        }
      }
    }
}
