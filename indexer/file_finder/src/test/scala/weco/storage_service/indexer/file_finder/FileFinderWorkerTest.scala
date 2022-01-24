package weco.storage_service.indexer.file_finder

import io.circe.Encoder
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.JsonUtil._
import weco.messaging.memory.MemoryMessageSender
import weco.messaging.worker.models.{RetryableFailure, Successful}
import weco.storage_service.BagRegistrationNotification
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.generators.StorageManifestGenerators
import weco.storage_service.indexer.models.FileContext
import weco.storage_service.indexer.file_finder.fixtures.WorkerServiceFixture

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

    withBagTrackerClient(dao) { bagTrackerClient =>
      withWorkerService(
        messageSender = messageSender,
        bagTrackerClient = bagTrackerClient
      ) { service =>
        val future =
          service.processMessage(
            BagRegistrationNotification(manifest)
          )

        whenReady(future) {
          _ shouldBe a[Successful[_]]
        }

        messageSender.messages should have size 1
        messageSender
          .getMessages[Seq[FileContext]]()
          .head should contain theSameElementsAs expectedMessages
      }
    }
  }

  it("skips files from previous versions") {
    val messageSender = new MemoryMessageSender()
    val dao = createStorageManifestDao()

    val v1Files = (1 to 3).map { _ =>
      createStorageManifestFileWith(pathPrefix = "v1")
    }
    val v2Files = (1 to 2).map { _ =>
      createStorageManifestFileWith(pathPrefix = "v2")
    }

    val manifest = createStorageManifestWith(
      version = BagVersion(2),
      files = v1Files ++ v2Files
    )
    dao.put(manifest) shouldBe a[Right[_, _]]

    val expectedMessages = v2Files.map { file =>
      FileContext(manifest = manifest, file = file)
    }

    withBagTrackerClient(dao) { bagTrackerClient =>
      withWorkerService(
        messageSender = messageSender,
        bagTrackerClient = bagTrackerClient
      ) { service =>
        val future =
          service.processMessage(
            BagRegistrationNotification(manifest)
          )

        whenReady(future) {
          _ shouldBe a[Successful[_]]
        }

        messageSender.messages should have size 1
        messageSender
          .getMessages[Seq[FileContext]]()
          .head should contain theSameElementsAs expectedMessages
      }
    }
  }

  it("splits the files into appropriately-sized batches") {
    val messageSender = new MemoryMessageSender()
    val dao = createStorageManifestDao()

    val manifest = createStorageManifestWith(
      version = BagVersion(1),
      files = (1 to 100).map { _ =>
        createStorageManifestFileWith(pathPrefix = "v1")
      }
    )
    dao.put(manifest) shouldBe a[Right[_, _]]

    withBagTrackerClient(dao) { bagTrackerClient =>
      withWorkerService(
        messageSender = messageSender,
        bagTrackerClient = bagTrackerClient,
        batchSize = 10
      ) { service =>
        val future =
          service.processMessage(
            BagRegistrationNotification(manifest)
          )

        whenReady(future) {
          _ shouldBe a[Successful[_]]
        }

        messageSender.messages should have size 10
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
        _ shouldBe a[RetryableFailure[_]]
      }
    }
  }

  it("fails if it cannot send a message") {
    val brokenSender = new MemoryMessageSender() {
      override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
        Failure(new Throwable("BOOM!"))
    }
    val dao = createStorageManifestDao()

    val manifest = createStorageManifestWith(
      version = BagVersion(1),
      files = Seq(
        createStorageManifestFileWith(pathPrefix = "v1")
      )
    )
    dao.put(manifest) shouldBe a[Right[_, _]]

    withBagTrackerClient(dao) { bagTrackerClient =>
      withWorkerService(
        messageSender = brokenSender,
        bagTrackerClient = bagTrackerClient
      ) { service =>
        val future =
          service.processMessage(
            BagRegistrationNotification(manifest)
          )

        whenReady(future) {
          _ shouldBe a[RetryableFailure[_]]
        }
      }
    }
  }
}
