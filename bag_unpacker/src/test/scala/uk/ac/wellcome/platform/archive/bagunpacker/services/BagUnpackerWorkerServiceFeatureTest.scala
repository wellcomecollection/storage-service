package uk.ac.wellcome.platform.archive.bagunpacker.services

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.WorkerServiceFixture
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions

class BagUnpackerWorkerServiceFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings
    with WorkerServiceFixture
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
              bagLocation = BagLocation(
                storageNamespace = "uploadNamespace",
                storagePrefix = Some("uploadPrefix"),
                unpackBagRequest.storageSpace,
                bagPath = BagPath("externalIdentifier")
              )
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
