package uk.ac.wellcome.platform.storage.ingests_worker

import java.net.URL
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Succeeded
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CallbackNotification,
  Ingest,
  IngestUpdate
}
import uk.ac.wellcome.platform.storage.ingests_tracker.client.AkkaIngestTrackerClient
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture
import uk.ac.wellcome.platform.storage.ingests_worker.fixtures.IngestsWorkerFixtures
import weco.http.fixtures.HttpFixtures

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

  override def contextUrl = new URL("http://www.example.com")

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
        withLocalSqsQueuePair(visibilityTimeout = 5) {
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
