package uk.ac.wellcome.platform.archive.bagunpacker

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.{BagUnpackerFixtures, CompressFixture}
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.generators.UnpackBagRequestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{BagRequest, Ingest}

class UnpackerFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings
    with BagUnpackerFixtures
    with UnpackBagRequestGenerators
    with IntegrationPatience
    with CompressFixture
    with IngestUpdateAssertions {

  it("receives and processes a notification") {
    val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()
    withBagUnpackerApp {
      case (_, srcBucket, queue, ingestTopic, outgoingTopic) =>
        withArchive(srcBucket, archiveFile) { archiveLocation =>

          val unpackBagRequest = createUnpackBagRequestWith(sourceLocation= archiveLocation)
          sendNotificationToSQS(queue, unpackBagRequest)

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

            assertTopicReceivesIngestEvent(
              requestId = unpackBagRequest.requestId,
              ingestTopic = ingestTopic
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

  it("sends a failed Ingest update if it cannot read the bag") {
    withBagUnpackerApp {
      case (_, _, queue, ingestTopic, outgoingTopic) =>
        val unpackBagRequest = createUnpackBagRequest()
        sendNotificationToSQS(queue, unpackBagRequest)

        eventually {
          assertSnsReceivesNothing(outgoingTopic)

          assertTopicReceivesIngestStatus(
            requestId = unpackBagRequest.requestId,
            ingestTopic = ingestTopic,
            status = Ingest.Failed
          ) { events =>
            events.map { _.description } shouldBe List("Unpacker failed")
          }
        }
    }
  }
}
