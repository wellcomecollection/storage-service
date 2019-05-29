package uk.ac.wellcome.platform.archive.ingests

import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Completed
import uk.ac.wellcome.platform.archive.common.ingests.models.{CallbackNotification, IngestUpdate}
import uk.ac.wellcome.platform.archive.ingests.fixtures._

class IngestsFeatureTest
    extends FunSpec
    with Matchers
    with Eventually
    with IngestsFixtures
    with TryValues {

  it("updates an existing ingest status to Completed") {
    withConfiguredApp {
      case (queue, messageSender, table) =>
        withIngestTracker(table) { ingestTracker =>
          val ingest = ingestTracker.initialise(createIngest).success.value
          val someBagId = Some(createBagId)
          val ingestStatusUpdate =
            createIngestStatusUpdateWith(
              id = ingest.id,
              status = Completed,
              maybeBag = someBagId)

          sendNotificationToSQS[IngestUpdate](queue, ingestStatusUpdate)

          eventually {
            val expectedIngest = ingest.copy(
              status = Completed,
              events = ingestStatusUpdate.events,
              bag = someBagId
            )

            val expectedMessage = CallbackNotification(
              ingestId = ingest.id,
              callbackUri = ingest.callback.get.uri,
              payload = expectedIngest
            )

            messageSender.getMessages[CallbackNotification] shouldBe Seq(expectedMessage)

            assertIngestCreated(ingest, table)

            assertIngestRecordedRecentEvents(
              ingestStatusUpdate.id,
              ingestStatusUpdate.events.map(_.description),
              table
            )
          }
        }
    }
  }
}
