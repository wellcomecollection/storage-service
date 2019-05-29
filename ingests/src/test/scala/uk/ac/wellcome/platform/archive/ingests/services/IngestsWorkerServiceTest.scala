package uk.ac.wellcome.platform.archive.ingests.services

import io.circe.Encoder
import org.scalatest.{FunSpec, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.messaging.worker.models.{DeterministicFailure, Successful}
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.{Completed, Processing}
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IdConstraintError
import uk.ac.wellcome.platform.archive.ingests.fixtures.IngestsFixtures

import scala.util.{Failure, Try}

class IngestsWorkerServiceTest
    extends FunSpec
    with IngestGenerators
    with IngestsFixtures
    with TryValues {

  it("updates an existing ingest to Completed") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        val messageSender = new MemoryMessageSender()
        withIngestWorker(queue, table, messageSender) { service =>
          withIngestTracker(table) { ingestTracker =>
            val ingest = ingestTracker.initialise(createIngest).success.value
            val ingestStatusUpdate =
              createIngestStatusUpdateWith(
                id = ingest.id,
                status = Completed
              )

            service.processMessage(ingestStatusUpdate).success.value shouldBe a[Successful[_]]

            val expectedIngest = ingest.copy(
              status = Completed,
              events = ingestStatusUpdate.events,
              bag = ingestStatusUpdate.affectedBag
            )

            val callbackNotification = CallbackNotification(
              ingestId = ingest.id,
              callbackUri = ingest.callback.get.uri,
              payload = expectedIngest
            )

            messageSender.getMessages[CallbackNotification] shouldBe Seq(callbackNotification)

            assertIngestCreated(expectedIngest, table)

            assertIngestRecordedRecentEvents(
              id = ingestStatusUpdate.id,
              expectedEventDescriptions = ingestStatusUpdate.events.map {
                _.description
              },
              table = table
            )
          }
        }
      }
    }
  }

  it("adds multiple events to an ingest") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        val messageSender = new MemoryMessageSender()
        withIngestWorker(queue, table, messageSender) { service =>
          withIngestTracker(table) { ingestTracker =>
            val ingest = ingestTracker.initialise(createIngest).success.value

            val ingestStatusUpdate1 =
              createIngestStatusUpdateWith(
                id = ingest.id,
                status = Processing
              )

            val ingestStatusUpdate2 =
              createIngestStatusUpdateWith(
                id = ingest.id,
                status = Processing,
                maybeBag = ingestStatusUpdate1.affectedBag
              )

            val expectedIngest = ingest.copy(
              status = Completed,
              events = ingestStatusUpdate1.events ++ ingestStatusUpdate2.events,
              bag = ingestStatusUpdate1.affectedBag
            )

            service.processMessage(ingestStatusUpdate1).success.value shouldBe a[Successful[_]]
            service.processMessage(ingestStatusUpdate2).success.value shouldBe a[Successful[_]]

            assertIngestCreated(expectedIngest, table)

            val expectedEventDescriptions =
              (ingestStatusUpdate1.events ++ ingestStatusUpdate2.events)
                .map { _.description }

            assertIngestRecordedRecentEvents(
              id = ingestStatusUpdate1.id,
              expectedEventDescriptions = expectedEventDescriptions,
              table = table
            )
          }
        }
      }
    }
  }

  it("fails if the ingest is not in the table") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        val messageSender = new MemoryMessageSender()
          withIngestWorker(queue, table, messageSender) { service =>
            val ingestStatusUpdate =
              createIngestStatusUpdateWith(
                status = Completed
              )

            val result = service.processMessage(ingestStatusUpdate)
            result.success.value shouldBe a[DeterministicFailure[_]]

            result
              .success.value
              .asInstanceOf[DeterministicFailure[_]]
              .failure shouldBe a[IdConstraintError]
        }
      }
    }
  }

  it("fails if publishing a message fails") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        val exception = new Throwable("BOOM!")

        val brokenSender = new MemoryMessageSender() {
          override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
            Failure(exception)
        }

        withIngestWorker(queue, table, brokenSender) { service =>
          withIngestTracker(table) { ingestTracker =>
            val ingest = ingestTracker.initialise(createIngest).success.value
            val ingestStatusUpdate =
              createIngestStatusUpdateWith(
                id = ingest.id,
                status = Completed
              )

            val result = service.processMessage(ingestStatusUpdate)

            result.success.value shouldBe a[DeterministicFailure[_]]
            result
              .success.value
              .asInstanceOf[DeterministicFailure[_]]
              .failure shouldBe exception
          }
        }
      }
    }
  }
}
