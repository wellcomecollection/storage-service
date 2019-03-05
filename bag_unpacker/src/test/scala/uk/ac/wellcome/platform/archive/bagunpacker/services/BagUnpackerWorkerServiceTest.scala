package uk.ac.wellcome.platform.archive.bagunpacker.services

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.{
  CompressFixture,
  WorkerServiceFixture
}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagLocation,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions

class BagUnpackerWorkerServiceTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings
    with WorkerServiceFixture
    with IntegrationPatience
    with CompressFixture
    with ProgressUpdateAssertions {

  it("receives and processes a notification") {
    withApp {
      case (srcBucket, queue, progressTopic, outgoingTopic) =>
        withArchive(srcBucket) { testArchive =>
          val requestId = randomUUID

          withBagNotification(
            queue,
            srcBucket,
            requestId,
            testArchive
          ) { unpackBagRequest =>
            eventually {

              val expectedNotification = BagRequest(
                requestId = unpackBagRequest.requestId,
                bagLocation = BagLocation(
                  storageNamespace = srcBucket.name,
                  storagePrefix = None,
                  storageSpace = unpackBagRequest.storageSpace,
                  bagPath = BagPath(unpackBagRequest.requestId.toString)
                )
              )

              assertSnsReceivesOnly[BagRequest](
                expectedNotification,
                outgoingTopic
              )

              assertTopicReceivesProgressEventUpdate(
                requestId = unpackBagRequest.requestId,
                progressTopic = progressTopic
              ) { events =>
                events.map {
                  _.description
                } shouldBe List(
                  "Unpacker succeeded"
                )
              }
            }
          }
        }
    }
  }
}
