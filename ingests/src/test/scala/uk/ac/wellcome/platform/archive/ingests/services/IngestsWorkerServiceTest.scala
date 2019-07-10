package uk.ac.wellcome.platform.archive.ingests.services

import io.circe.Encoder
import org.scalatest.{FunSpec, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.messaging.worker.models.{DeterministicFailure, Successful}
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.{
  Completed,
  Processing
}
import uk.ac.wellcome.platform.archive.common.ingests.tracker.fixtures.IngestTrackerFixtures
import uk.ac.wellcome.platform.archive.ingests.fixtures.IngestsFixtures

import scala.util.{Failure, Try}

class IngestsWorkerServiceTest
    extends FunSpec
    with IngestGenerators
    with IngestsFixtures
    with IngestTrackerFixtures
    with TryValues {

  it("updates an existing ingest to Completed") {
    val ingest = createIngest

    withLocalSqsQueue { queue =>
      withMemoryIngestTracker(initialIngests = Seq(ingest)) {
        implicit ingestTracker =>
          val messageSender = new MemoryMessageSender()
          withIngestWorker(queue, ingestTracker, messageSender) { service =>
            val ingestStatusUpdate =
              createIngestStatusUpdateWith(
                id = ingest.id,
                status = Completed
              )

            service
              .processMessage(ingestStatusUpdate)
              .success
              .value shouldBe a[Successful[_]]

            val expectedIngest = ingest.copy(
              status = Completed,
              events = ingestStatusUpdate.events
            )

            val callbackNotification = CallbackNotification(
              ingestId = ingest.id,
              callbackUri = ingest.callback.get.uri,
              payload = expectedIngest
            )

            messageSender.getMessages[CallbackNotification] shouldBe Seq(
              callbackNotification)

            assertIngestCreated(expectedIngest)

            assertIngestRecordedRecentEvents(
              id = ingestStatusUpdate.id,
              expectedEventDescriptions = ingestStatusUpdate.events.map {
                _.description
              }
            )
          }
      }
    }
  }

  it("adds multiple events to an ingest") {
    val ingest = createIngest

    withLocalSqsQueue { queue =>
      withMemoryIngestTracker(initialIngests = Seq(ingest)) {
        implicit ingestTracker =>
          val messageSender = new MemoryMessageSender()
          withIngestWorker(queue, ingestTracker, messageSender) { service =>
            val ingestStatusUpdate1 =
              createIngestStatusUpdateWith(
                id = ingest.id,
                status = Processing
              )

            val ingestStatusUpdate2 =
              createIngestStatusUpdateWith(
                id = ingest.id,
                status = Processing
              )

            val expectedIngest = ingest.copy(
              status = Completed,
              events = ingestStatusUpdate1.events ++ ingestStatusUpdate2.events
            )

            service
              .processMessage(ingestStatusUpdate1)
              .success
              .value shouldBe a[Successful[_]]
            service
              .processMessage(ingestStatusUpdate2)
              .success
              .value shouldBe a[Successful[_]]

            assertIngestCreated(expectedIngest)

            val expectedEventDescriptions =
              (ingestStatusUpdate1.events ++ ingestStatusUpdate2.events)
                .map {
                  _.description
                }

            assertIngestRecordedRecentEvents(
              id = ingestStatusUpdate1.id,
              expectedEventDescriptions = expectedEventDescriptions
            )
          }
      }
    }
  }

  it("fails if the ingest is not in the tracker") {
    withLocalSqsQueue { queue =>
      withMemoryIngestTracker() { implicit ingestTracker =>
        val messageSender = new MemoryMessageSender()
        withIngestWorker(queue, ingestTracker, messageSender) { service =>
          val ingestStatusUpdate =
            createIngestStatusUpdateWith(
              status = Completed
            )

          val result = service.processMessage(ingestStatusUpdate)
          result.success.value shouldBe a[DeterministicFailure[_]]

          result.success.value
            .asInstanceOf[DeterministicFailure[_]]
            .failure shouldBe a[Throwable]
        }
      }
    }
  }

  it("fails if publishing a message fails") {
    val ingest = createIngest

    withLocalSqsQueue { queue =>
      withMemoryIngestTracker(initialIngests = Seq(ingest)) {
        implicit ingestTracker =>
          val exception = new Throwable("BOOM!")

          val brokenSender = new MemoryMessageSender() {
            override def sendT[T](t: T)(
              implicit encoder: Encoder[T]): Try[Unit] =
              Failure(exception)
          }

          withIngestWorker(queue, ingestTracker, brokenSender) { service =>
            val ingestStatusUpdate =
              createIngestStatusUpdateWith(
                id = ingest.id,
                status = Completed
              )

            val result = service.processMessage(ingestStatusUpdate)

            result.success.value shouldBe a[DeterministicFailure[_]]
            result.success.value
              .asInstanceOf[DeterministicFailure[_]]
              .failure shouldBe exception
          }
      }
    }
  }
}
