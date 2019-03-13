package uk.ac.wellcome.platform.archive.ingests

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.Completed
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CallbackNotification,
  IngestUpdate
}
import uk.ac.wellcome.platform.archive.ingests.fixtures._

class IngestsFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with IngestsFixture
    with IntegrationPatience {

  it("updates an existing ingest status to Completed") {
    withConfiguredApp {
      case (queue, topic, table) => {
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
              val actualMessage =
                notificationMessage[CallbackNotification](topic)
              actualMessage.id shouldBe ingest.id
              actualMessage.callbackUri shouldBe ingest.callback.get.uri

              val expectedIngest = ingest.copy(
                status = Completed,
                events = ingestStatusUpdate.events,
                bag = someBagId
              )
              actualMessage.payload shouldBe expectedIngest

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
}
