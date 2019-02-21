package uk.ac.wellcome.platform.archive.bagunpacker

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.BagUnpackerFixtures
import uk.ac.wellcome.platform.archive.common.models.BagRequest

class BagUnpackerFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings
    with BagUnpackerFixtures
    with IntegrationPatience
    with ProgressUpdateAssertions {

  it("receives and forwards a notification") {
    withApp {
      case (sourceBucket, queue, progressTopic, outgoingTopic) =>
        val requestId = randomUUID
        withBagNotification(
          queue,
          sourceBucket,
          requestId
        ) { unpackBagRequest =>
          eventually {

            val expectedNotification = BagRequest(
              archiveRequestId = unpackBagRequest.requestId,
              srcBagLocation = unpackBagRequest.bagDestination
            )

            assertSnsReceivesOnly(
              expectedNotification,
              outgoingTopic
            )
          }
        }
    }
  }
}
