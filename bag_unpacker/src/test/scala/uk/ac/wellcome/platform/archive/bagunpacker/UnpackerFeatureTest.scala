package uk.ac.wellcome.platform.archive.bagunpacker

import java.nio.file.Paths

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.{
  BagUnpackerFixtures,
  CompressFixture
}
import uk.ac.wellcome.platform.archive.common.UnpackedBagLocationPayload
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  Ingest,
  IngestStatusUpdate
}
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

class UnpackerFeatureTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with BagUnpackerFixtures
    with IntegrationPatience
    with CompressFixture[Bucket]
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("receives and processes a notification") {
    val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()
    withBagUnpackerApp(stepName = "unpacker") {
      case (_, dstBucket, queue, ingests, outgoing) =>
        withStreamStore { implicit streamStore =>
          withLocalS3Bucket { srcBucket =>
            withArchive(srcBucket, archiveFile) { archiveLocation =>
              val sourceLocationPayload =
                createSourceLocationPayloadWith(archiveLocation)
              sendNotificationToSQS(queue, sourceLocationPayload)

              eventually {
                val expectedPayload = UnpackedBagLocationPayload(
                  context = sourceLocationPayload.context,
                  unpackedBagLocation = ObjectLocationPrefix(
                    namespace = dstBucket.name,
                    path = Paths
                      .get(
                        sourceLocationPayload.storageSpace.toString,
                        sourceLocationPayload.ingestId.toString
                      )
                      .toString
                  )
                )

                outgoing
                  .getMessages[UnpackedBagLocationPayload]
                  .distinct shouldBe Seq(expectedPayload)

                assertTopicReceivesIngestUpdates(
                  sourceLocationPayload.ingestId,
                  ingests
                ) { ingestUpdates =>
                  val eventDescriptions: Seq[String] =
                    ingestUpdates
                      .flatMap { _.events }
                      .map { _.description }
                      .distinct

                  eventDescriptions should have size 2

                  eventDescriptions(0) shouldBe "Unpacker started"
                  eventDescriptions(1) should fullyMatch regex """Unpacker succeeded - Unpacked \d+ [KM]B from \d+ files"""
                }
              }
            }
          }
        }
    }
  }

  it("sends a failed Ingest update if it cannot read the bag") {
    withBagUnpackerApp(stepName = "unpacker") {
      case (_, _, queue, ingests, outgoing) =>
        val sourceLocation = createObjectLocationWith(
          bucket = createBucket
        )

        val payload = createSourceLocationPayloadWith(
          sourceLocation = sourceLocation
        )
        sendNotificationToSQS(queue, payload)

        eventually {
          outgoing.messages shouldBe empty

          assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
            ingestUpdates =>
              ingestUpdates.size shouldBe 2

              val ingestStart = ingestUpdates.head
              ingestStart.events.head.description shouldBe "Unpacker started"

              val ingestFailed =
                ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
              ingestFailed.status shouldBe Ingest.Failed
              ingestFailed.events.head.description shouldBe
                s"Unpacker failed - There is no S3 bucket ${sourceLocation.namespace}"
          }
        }
    }
  }
}
