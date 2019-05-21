package uk.ac.wellcome.platform.archive.ingests

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Completed
import uk.ac.wellcome.platform.archive.common.ingests.models.{CallbackNotification, IngestUpdate}
import uk.ac.wellcome.platform.archive.ingests.fixtures._

class IngestsFeatureTest
    extends FunSpec
    with Matchers
    with IngestsFixtures {

  it("updates an existing ingest status to Completed") {
    withConfiguredApp {
      case (queue, messageSender, table) =>
        withIngestTracker(table) { monitor =>
          withIngest(monitor) { ingest =>
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

              messageSender.messages
                .map { _.body }
                .map { fromJson[CallbackNotification](_).get } shouldBe Seq(expectedMessage)

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
}
