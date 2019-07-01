package uk.ac.wellcome.platform.archive.ingests

import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Completed
import uk.ac.wellcome.platform.archive.common.ingests.models.{CallbackNotification, IngestUpdate}
import uk.ac.wellcome.platform.archive.ingests.fixtures._

class IngestsFeatureTest
    extends FunSpec
    with Matchers
    with Eventually
    with IngestsFixtures
    with IngestGenerators
    with TryValues {

  it("updates an existing ingest status to Completed") {
    val ingest = createIngest

    withConfiguredApp(initialIngests = Seq(ingest)) {
      case (queue, messageSender, ingestTracker) =>
        implicit val _ = ingestTracker

        val ingestStatusUpdate =
          createIngestStatusUpdateWith(id = ingest.id, status = Completed)

        sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

        eventually {
          val expectedIngest = ingest.copy(
            status = Completed,
            events = ingestStatusUpdate.events
          )

          val expectedMessage = CallbackNotification(
            ingestId = ingest.id,
            callbackUri = ingest.callback.get.uri,
            payload = expectedIngest
          )

          messageSender.getMessages[CallbackNotification] shouldBe Seq(
            expectedMessage)

          assertIngestCreated(ingest)

          assertIngestRecordedRecentEvents(
            ingestStatusUpdate.id,
            ingestStatusUpdate.events.map { _.description }
          )
        }
    }
  }
}
