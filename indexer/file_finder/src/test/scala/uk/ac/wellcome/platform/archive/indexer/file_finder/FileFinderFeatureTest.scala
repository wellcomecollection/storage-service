package uk.ac.wellcome.platform.archive.indexer.file_finder

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.platform.archive.indexer.file_finder.fixtures.WorkerServiceFixture

class FileFinderFeatureTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with IntegrationPatience
    with WorkerServiceFixture
    with StorageManifestGenerators {

  it("sends all the files from a bag") {
    val messageSender = new MemoryMessageSender()
    val dao = createStorageManifestDao()

    val manifest = createStorageManifestWith(
      version = BagVersion(1),
      files = Seq(
        createStorageManifestFileWith(pathPrefix = "v1")
      )
    )
    dao.put(manifest) shouldBe a[Right[_, _]]

    val expectedMessages = manifest.manifest.files.map { file =>
      FileContext(manifest = manifest, file = file)
    }

    withLocalSqsQueue() { queue =>
      withBagTrackerClient(dao) { bagTrackerClient =>
        withWorkerService(queue, messageSender, bagTrackerClient) { _ =>
          sendNotificationToSQS(
            queue,
            BagRegistrationNotification(manifest)
          )

          eventually {
            messageSender.messages should have size 1
            messageSender.getMessages[Seq[FileContext]]().head should contain theSameElementsAs expectedMessages

            assertQueueEmpty(queue)
          }
        }
      }
    }
  }
}
