package uk.ac.wellcome.platform.storage.ingests.api

import io.circe.Encoder
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.IngestRequestPayload
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.MemoryIngestTracker
import uk.ac.wellcome.platform.storage.ingests.api.fixtures.IngestsStarterFixtures

import scala.util.{Failure, Success, Try}

class IngestStarterTest
    extends FunSpec
    with Matchers
    with IngestGenerators
    with IngestsStarterFixtures {

  val ingest: Ingest = createIngest

  it("saves an Ingest and sends a notification") {
    val sender = createMessageSender
    val tracker = createIngestTracker

    val starter = createIngestStarter(sender, tracker)

    starter.initialise(ingest) shouldBe Success(ingest)

    tracker.ingests shouldBe Map(ingest.id -> ingest)

    val expectedPayload = IngestRequestPayload(ingest)
    sender.getMessages[IngestRequestPayload]() shouldBe Seq(expectedPayload)
  }

  it("returns a failed future if saving to the tracker fails") {
    val sender = createMessageSender

    val tracker = new MemoryIngestTracker() {
      override def initialise(ingest: Ingest): Try[Ingest] = Failure(new Throwable("BOOM!"))
    }

    val starter = createIngestStarter(sender, tracker)

    starter.initialise(ingest) shouldBe a[Failure[_]]

    tracker.ingests shouldBe empty
    sender.messages shouldBe empty
  }

  it("returns a failed future if sending the onward message fails") {
    val sender = new MemoryMessageSender(
      destination = randomAlphanumeric(),
      subject = randomAlphanumeric()
    ) {
      override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] = Failure(new Throwable("BOOM!"))
    }

    val tracker = createIngestTracker

    val starter = createIngestStarter(sender, tracker)

    starter.initialise(ingest) shouldBe a[Failure[_]]

    sender.messages shouldBe empty
  }
}
