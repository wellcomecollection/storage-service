package uk.ac.wellcome.platform.archive.bagverifier

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions

class BagVerifierFeatureTest
  extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings
    with BagVerifierFixtures
    with ProgressUpdateAssertions {

  it("receives a notification") {
    withApp {
      case (
        sourceBucket,
        queue,
        progressTopic,
        outgoingTopic) =>
        val requestId = randomUUID
        withBagNotification(queue, sourceBucket, requestId) { bagLocation =>
          eventually {

            true shouldBe(false)
          }
        }
    }
  }
}
