package uk.ac.wellcome.platform.archive.indexer.file_finder

import io.circe.Encoder
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.messaging.worker.models.{NonDeterministicFailure, Successful}
import uk.ac.wellcome.platform.archive.common.BagRegistrationNotification
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.platform.archive.indexer.file_finder.fixtures.WorkerServiceFixture

import scala.util.{Failure, Try}

class FileFinderWorkerTest
  extends AnyFunSpec
    with Matchers
    with ScalaFutures
    with IntegrationPatience
    with WorkerServiceFixture
    with StorageManifestGenerators {

  it("processes a manifest with a single file") {
    val messageSender = new MemoryMessageSender()
    val dao = createStorageManifestDao()

    val manifest = createStorageManifestWithFileCount(fileCount = 3)
    dao.put(manifest) shouldBe a[Right[_, _]]

    val expectedMessages = manifest.manifest.files.map { file =>
      FileContext(manifest = manifest, file = file)
    }

    withBagTrackerClient(dao) { bagTrackerClient =>
      withWorkerService(messageSender = messageSender, bagTrackerClient = bagTrackerClient) { service =>
        val future =
          service.processMessage(
            BagRegistrationNotification(manifest)
          )

        whenReady(future) {
          _ shouldBe a[Successful[_]]
        }

        messageSender.messages should have size 3
        messageSender.getMessages[FileContext]() should contain theSameElementsAs expectedMessages
      }
    }
  }

  it("fails if it cannot parse the Notification version") {
    withWorkerService { service =>
      val future =
        service.processMessage(
          BagRegistrationNotification(
            space = createStorageSpace,
            externalIdentifier = createExternalIdentifier,
            version = "notaversionstring"
          )
        )

      whenReady(future.failed) {
        _ shouldBe a[IllegalArgumentException]
      }
    }
  }

  it("fails if asked to look up a non-existent bag") {
    withWorkerService { service =>
      val future =
        service.processMessage(
          BagRegistrationNotification(createStorageManifest)
        )

      whenReady(future) {
        _ shouldBe a[NonDeterministicFailure[_]]
      }
    }
  }

  it("fails if it cannot send a message") {
    val brokenSender = new MemoryMessageSender() {
      override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
        Failure(new Throwable("BOOM!"))
    }
    val dao = createStorageManifestDao()

    val manifest = createStorageManifestWithFileCount(fileCount = 3)
    dao.put(manifest) shouldBe a[Right[_, _]]

    withBagTrackerClient(dao) { bagTrackerClient =>
      withWorkerService(messageSender = brokenSender, bagTrackerClient = bagTrackerClient) { service =>
        val future =
          service.processMessage(
            BagRegistrationNotification(manifest)
          )

        whenReady(future) {
          _ shouldBe a[NonDeterministicFailure[_]]
        }
      }
    }
  }
}
