package uk.ac.wellcome.platform.storage.ingests_worker

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.QueuePair
import uk.ac.wellcome.platform.archive.common.fixtures.HttpFixtures
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Succeeded
import uk.ac.wellcome.platform.archive.common.ingests.models.{CallbackNotification, Ingest, IngestUpdate}
import uk.ac.wellcome.platform.storage.ingests_tracker.client.AkkaIngestTrackerClient
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture
import uk.ac.wellcome.platform.storage.ingests_worker.fixtures.IngestsWorkerFixtures

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

  //TODO: when we have AkkaIngestTrackerClient tests we can substitute this for a fake
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

        withLocalSqsQueueAndDlqAndTimeout(visibilityTimeout = 5) {
          case QueuePair(queue, _) =>

            withIngestWorker(queue, ingestTrackerClient) { _ =>

              whenGetRequestReady(healthcheckPath) { healthcheck =>

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
                  val storedIngest = ingestsTracker.get(ingest.id).right.value.identifiedT
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
