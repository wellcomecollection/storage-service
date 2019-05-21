package uk.ac.wellcome.platform.archive.ingests.services

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import io.circe.Encoder
import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.messaging.worker.models.DeterministicFailure
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.CallbackNotification
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.{Completed, Processing}
import uk.ac.wellcome.platform.archive.ingests.fixtures.IngestsFixtures

import scala.util.{Failure, Success, Try}

class IngestsWorkerServiceTest
    extends FunSpec
    with IngestGenerators
    with IngestsFixtures {
  it("updates an existing ingest to Completed") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        val messageSender = createMessageSender
        withIngestWorker(queue, table, messageSender) { service =>
          withIngestTracker(table) { monitor =>
            withIngest(monitor) { ingest =>
              val ingestStatusUpdate =
                createIngestStatusUpdateWith(
                  id = ingest.id,
                  status = Completed
                )

              service.processMessage(ingestStatusUpdate) shouldBe a[Success[_]]

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

              messageSender.messages
                .map { _.body }
                .map { fromJson[CallbackNotification](_).get } shouldBe Seq(callbackNotification)

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
  }

  it("adds multiple events to an ingest") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        val messageSender = createMessageSender
        withIngestWorker(queue, table, messageSender) { service =>
          withIngestTracker(table) { monitor =>
            withIngest(monitor) { ingest =>
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

              service.processMessage(ingestStatusUpdate1) shouldBe a[Success[_]]
              service.processMessage(ingestStatusUpdate2) shouldBe a[Success[_]]

              assertIngestCreated(expectedIngest, table)

              val expectedEventDescriptions =
                (ingestStatusUpdate1.events ++ ingestStatusUpdate2.events)
                  .map {
                    _.description
                  }

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
  }

  it("fails if the ingest is not in the table") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        val messageSender = createMessageSender
        withIngestWorker(queue, table, messageSender) { service =>
          val ingestStatusUpdate =
            createIngestStatusUpdateWith(
              status = Completed
            )

          val result = service.processMessage(ingestStatusUpdate)
          result shouldBe a[Success[_]]

          val err = result.get

          err shouldBe a[DeterministicFailure[_]]
          err
            .asInstanceOf[DeterministicFailure[_]]
            .failure shouldBe a[ConditionalCheckFailedException]
        }
      }
    }
  }

  it("fails if publishing a message fails") {
    withLocalSqsQueue { queue =>
      withIngestTrackerTable { table =>
        val brokenSender = new MemoryMessageSender(
          destination = randomAlphanumeric(),
          subject = randomAlphanumeric()
        ) {
          override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] = Failure(new Throwable("BOOM!"))
        }

        withIngestWorker(queue, table, brokenSender) { service =>
          withIngestTracker(table) { monitor =>
            withIngest(monitor) { ingest =>
              val ingestStatusUpdate =
                createIngestStatusUpdateWith(
                  id = ingest.id,
                  status = Completed
                )

              val result = service.processMessage(ingestStatusUpdate)
              result shouldBe a[Success[_]]

              val err = result.get
              err shouldBe a[DeterministicFailure[_]]
              err
                .asInstanceOf[DeterministicFailure[_]]
                .failure shouldBe a[Throwable]

            }
          }
        }
      }
    }
  }
}
