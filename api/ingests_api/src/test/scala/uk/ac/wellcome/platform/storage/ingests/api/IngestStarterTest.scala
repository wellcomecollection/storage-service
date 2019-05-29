package uk.ac.wellcome.platform.storage.ingests.api

import io.circe.Encoder
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.IngestRequestPayload
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.storage.ingests.api.fixtures.IngestStarterFixture
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

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
    withIngestTrackerTable { table =>
      withIngestStarter(table, messageSender) { ingestStarter =>
        val result = ingestStarter.initialise(ingest)
        result.success.value shouldBe ingest

        assertTableOnlyHasItem(ingest, table)

        val expectedPayload = IngestRequestPayload(ingest)
        messageSender.getMessages[IngestRequestPayload] shouldBe Seq(
          expectedPayload)
      }
    }
  }

  it("returns a failed future if saving to DynamoDB fails") {
    val messageSender = new MemoryMessageSender()
    val fakeTable = Table("does-not-exist", index = "does-not-exist")

    withIngestStarter(fakeTable, messageSender) { ingestStarter =>
      val result = ingestStarter.initialise(ingest)

      result shouldBe a[Failure[_]]

      messageSender.messages shouldBe empty
    }
  }

  it("returns a failed future if sending a message fails") {
    withIngestTrackerTable { table =>
      val brokenMessageSender = new MemoryMessageSender() {
        override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
          Failure(new Throwable("BOOM!"))
      }

      withIngestStarter(table, brokenMessageSender) { ingestStarter =>
        val result = ingestStarter.initialise(ingest)

        result shouldBe a[Failure[_]]

        assertTableOnlyHasItem(ingest, table)
      }
    }
  }
}
