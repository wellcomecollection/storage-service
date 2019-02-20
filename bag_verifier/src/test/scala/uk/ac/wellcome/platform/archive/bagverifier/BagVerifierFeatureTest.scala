package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.json.JsonUtil._

class BagVerifierFeatureTest
  extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings
    with BagVerifierFixtures
    with IntegrationPatience
    with ProgressUpdateAssertions {

  it("receives and forwards a notification") {
    withApp {
      case (
        sourceBucket,
        queue,
        progressTopic,
        outgoingTopic) =>
        val requestId = randomUUID
        withBagNotification(
          queue, sourceBucket, requestId
        ) { replicationRequest =>
          eventually {

            assertSnsReceivesOnly(
              replicationRequest,
              outgoingTopic
            )
          }
        }
    }
  }
}
