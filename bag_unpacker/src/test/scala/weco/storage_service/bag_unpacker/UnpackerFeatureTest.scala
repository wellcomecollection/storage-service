package weco.storage_service.bag_unpacker

import java.nio.file.Paths

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.JsonUtil._
import weco.storage_service.bag_unpacker.fixtures.BagUnpackerFixtures
import weco.storage_service.bag_unpacker.fixtures.s3.S3CompressFixture
import weco.storage_service.UnpackedBagLocationPayload
import weco.storage_service.generators.PayloadGenerators
import weco.storage_service.ingests.fixtures.IngestUpdateAssertions
import weco.storage_service.ingests.models.{Ingest, IngestStatusUpdate}
import weco.storage.providers.s3.S3ObjectLocationPrefix

class UnpackerFeatureTest
    extends AnyFunSpec
    with Matchers
    with Eventually
    with BagUnpackerFixtures
    with IntegrationPatience
    with S3CompressFixture
    with IngestUpdateAssertions
    with PayloadGenerators {

  it("receives and processes a notification") {
    val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()
    withBagUnpackerApp(stepName = "unpacker") {
      case (dstBucket, queue, ingests, outgoing) =>
        withStreamStore { implicit streamStore =>
          withLocalS3Bucket { srcBucket =>
            withArchive(srcBucket, archiveFile) { archiveLocation =>
              val sourceLocationPayload =
                createSourceLocationPayloadWith(archiveLocation)
              sendNotificationToSQS(queue, sourceLocationPayload)

              eventually {
                val expectedPayload = UnpackedBagLocationPayload(
                  context = sourceLocationPayload.context,
                  unpackedBagLocation = S3ObjectLocationPrefix(
                    bucket = dstBucket.name,
                    keyPrefix = Paths
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

                assertTopicReceivesIngestUpdates(ingests) { ingestUpdates =>
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
      case (_, queue, ingests, outgoing) =>
        val sourceLocation = createS3ObjectLocationWith(
          bucket = createBucket
        )

        val payload = createSourceLocationPayloadWith(
          sourceLocation = sourceLocation
        )
        sendNotificationToSQS(queue, payload)

        eventually {
          outgoing.messages shouldBe empty

          assertTopicReceivesIngestUpdates(ingests) { ingestUpdates =>
            ingestUpdates.size shouldBe 2

            val ingestStart = ingestUpdates.head
            ingestStart.events.head.description shouldBe "Unpacker started"

            val ingestFailed =
              ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
            ingestFailed.status shouldBe Ingest.Failed
            ingestFailed.events.head.description shouldBe
              s"Unpacker failed - There is no S3 bucket ${sourceLocation.bucket}"
          }
        }
    }
  }
}
