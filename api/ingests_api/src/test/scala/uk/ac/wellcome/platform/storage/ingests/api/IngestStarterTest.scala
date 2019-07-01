package uk.ac.wellcome.platform.storage.ingests.api

import io.circe.Encoder
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.SourceLocationPayload
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.IngestTrackerStoreError
import uk.ac.wellcome.platform.archive.common.ingests.tracker.memory.MemoryIngestTracker
import uk.ac.wellcome.platform.storage.ingests.api.fixtures.IngestStarterFixture
import uk.ac.wellcome.storage.{StoreWriteError, Version}
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

import scala.util.{Failure, Try}

class IngestStarterTest
    extends FunSpec
    with Matchers
    with IngestStarterFixture
    with IngestGenerators
    with TryValues {

  val ingest: Ingest = createIngest

  it("saves an Ingest and sends a notification") {
    val messageSender = new MemoryMessageSender()
    withMemoryIngestTracker(initialIngests = Seq.empty) { ingestTracker =>
      withIngestStarter(ingestTracker, messageSender) { ingestStarter =>
        val result = ingestStarter.initialise(ingest)
        result.success.value shouldBe ingest

        ingestTracker.underlying
          .getLatest(ingest.id)
          .right
          .value
          .identifiedT shouldBe ingest

        val expectedPayload = SourceLocationPayload(ingest)
        messageSender.getMessages[SourceLocationPayload] shouldBe Seq(
          expectedPayload)
      }
    }
  }

  it("returns a failed future if saving to DynamoDB fails") {
    val messageSender = new MemoryMessageSender()

    val brokenTracker = new MemoryIngestTracker(
      underlying = new MemoryVersionedStore[IngestID, Int, Ingest](
        new MemoryStore[Version[IngestID, Int], Ingest](
          initialEntries = Map.empty) with MemoryMaxima[IngestID, Ingest]
      )
    ) {
      override def init(ingest: Ingest): Result =
        Left(IngestTrackerStoreError(StoreWriteError(new Throwable("BOOM!"))))
    }

    withIngestStarter(brokenTracker, messageSender) { ingestStarter =>
      val result = ingestStarter.initialise(ingest)

      result shouldBe a[Failure[_]]

      messageSender.messages shouldBe empty
    }
  }

  it("returns a failed future if sending a message fails") {
    withMemoryIngestTracker(initialIngests = Seq.empty) { ingestTracker =>
      val brokenMessageSender = new MemoryMessageSender() {
        override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
          Failure(new Throwable("BOOM!"))
      }

      withIngestStarter(ingestTracker, brokenMessageSender) { ingestStarter =>
        val result = ingestStarter.initialise(ingest)

        result shouldBe a[Failure[_]]

        ingestTracker.underlying
          .getLatest(ingest.id)
          .right
          .value
          .identifiedT shouldBe ingest
      }
    }
  }
}
