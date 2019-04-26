package uk.ac.wellcome.platform.archive.bagunpacker

import java.nio.file.Paths

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.{
  BagUnpackerFixtures,
  CompressFixture,
  UnpackBagRequestGenerators
}
import uk.ac.wellcome.platform.archive.common.ObjectLocationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest

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
          val unpackBagRequest =
            createUnpackBagRequestWith(sourceLocation = archiveLocation)
          sendNotificationToSQS(queue, unpackBagRequest)

          eventually {
            val expectedPayload = ObjectLocationPayload(
              ingestId = unpackBagRequest.ingestId,
              storageSpace = unpackBagRequest.storageSpace,
              objectLocation = createObjectLocationWith(
                bucket = srcBucket,
                key = Paths
                  .get(
                    unpackBagRequest.storageSpace.toString,
                    unpackBagRequest.ingestId.toString
                  )
                  .toString
              )
            )

            assertSnsReceivesOnly[ObjectLocationPayload](
              expectedPayload,
              outgoingTopic
            )

            assertTopicReceivesIngestEvent(
              ingestId = unpackBagRequest.ingestId,
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
        val unpackBagRequest = createUnpackBagRequest
        sendNotificationToSQS(queue, unpackBagRequest)

        eventually {
          assertSnsReceivesNothing(outgoingTopic)

          assertTopicReceivesIngestStatus(
            ingestId = unpackBagRequest.ingestId,
            ingestTopic = ingestTopic,
            status = Ingest.Failed
          ) { events =>
            events.map { _.description } shouldBe
              List(
                s"Unpacker failed - ${unpackBagRequest.sourceLocation} does not exist")
          }
        }
    }
  }
}
