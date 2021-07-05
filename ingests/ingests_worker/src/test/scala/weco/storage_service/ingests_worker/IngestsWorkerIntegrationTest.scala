package weco.storage_service.ingests_worker

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.JsonUtil._
import weco.messaging.fixtures.SQS.QueuePair
import weco.storage_service.generators.IngestGenerators
import weco.storage_service.ingests.models.Ingest.Succeeded
import weco.storage_service.ingests.models.{
  CallbackNotification,
  Ingest,
  IngestUpdate
}
import weco.storage_service.ingests_tracker.client.AkkaIngestTrackerClient
import weco.storage_service.ingests_tracker.fixtures.IngestsTrackerApiFixture
import weco.storage_service.ingests_worker.fixtures.IngestsWorkerFixtures
import weco.http.fixtures.HttpFixtures

import scala.concurrent.ExecutionContext.Implicits.global

class IngestsWorkerIntegrationTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with HttpFixtures
    with IngestsWorkerFixtures
    with IngestsTrackerApiFixture
    with IngestGenerators
    with IntegrationPatience {

  implicit val as = ActorSystem()

  val host = "http://localhost:8080"
  val healthcheckPath = s"$host/healthcheck"

  val ingestTrackerClient = new AkkaIngestTrackerClient(Uri(host))

  it("marks an ingest as Completed") {
    val ingest = createIngestWith(
      createdDate = Instant.now()
    )

    val ingestStatusUpdate =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Succeeded
      )

    val expectedIngest = ingest.copy(
      status = Succeeded,
      events = ingestStatusUpdate.events
    )

    val expectedCallbackNotification = CallbackNotification(
      ingestId = ingest.id,
      callbackUri = ingest.callback.get.uri,
      payload = expectedIngest
    )

    withIngestsTrackerApi(Seq(ingest)) {
      case (callbackSender, ingestsSender, ingestsTracker) =>
        withLocalSqsQueuePair() {
          case QueuePair(queue, _) =>
            withIngestWorker(queue, ingestTrackerClient) { _ =>
              whenAbsoluteGetRequestReady(healthcheckPath) { healthcheck =>
                // We know the TrackerApi is running
                healthcheck.status shouldBe StatusCodes.OK

                sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

                eventually {
                  callbackSender
                    .getMessages[CallbackNotification] shouldBe Seq(
                    expectedCallbackNotification
                  )

                  // reads messages from the queue
                  getMessages(queue) shouldBe empty

                  //updates the ingest tracker
                  val storedIngest =
                    ingestsTracker.get(ingest.id).value.identifiedT
                  storedIngest.status shouldBe Succeeded

                  // records the events in the ingest tracker
                  assertIngestRecordedRecentEvents(
                    ingestStatusUpdate.id,
                    ingestStatusUpdate.events.map {
                      _.description
                    }
                  )(ingestsTracker)

                  // sends a message with the updated ingest
                  ingestsSender.getMessages[Ingest] shouldBe Seq(
                    expectedIngest
                  )
                }
              }
            }
        }
    }
  }
}
