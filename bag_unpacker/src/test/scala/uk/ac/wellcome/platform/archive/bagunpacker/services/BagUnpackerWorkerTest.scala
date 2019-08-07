package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.nio.file.Paths

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.{BagUnpackerFixtures, CompressFixture}
import uk.ac.wellcome.platform.archive.common.UnpackedBagLocationPayload
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

class BagUnpackerWorkerTest
  extends FunSpec
    with Matchers
    with BagUnpackerFixtures
    with CompressFixture[Bucket]
    with IngestUpdateAssertions
    with PayloadGenerators {
  it("processes a message") {
    val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()
    withBagUnpackerApp(stepName = "unpacker") {
      case (_, srcBucket, queue, ingests, outgoing) =>
        withStreamStore { implicit streamStore =>
          withArchive(srcBucket, archiveFile) { archiveLocation =>
            val sourceLocationPayload =
              createSourceLocationPayloadWith(archiveLocation)
            sendNotificationToSQS(queue, sourceLocationPayload)

            eventually {
              val expectedPayload = UnpackedBagLocationPayload(
                context = sourceLocationPayload.context,
                unpackedBagLocation = ObjectLocationPrefix(
                  namespace = srcBucket.name,
                  path = Paths
                    .get(
                      sourceLocationPayload.storageSpace.toString,
                      sourceLocationPayload.ingestId.toString
                    )
                    .toString
                )
              )

              outgoing.getMessages[UnpackedBagLocationPayload] shouldBe Seq(
                expectedPayload)

              assertTopicReceivesIngestUpdates(
                sourceLocationPayload.ingestId,
                ingests) { ingestUpdates =>
                val eventDescriptions: Seq[String] =
                  ingestUpdates
                    .flatMap { _.events }
                    .map { _.description }
                    .distinct

                eventDescriptions should have size 2

                eventDescriptions(0) shouldBe "Unpacker started"
                eventDescriptions(1) should fullyMatch regex """Unpacker succeeded - Unpacked \d+ bytes from \d+ files"""
              }
            }
          }
        }
    }
  }

  def withWorker[R](
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    dstBucket: Bucket)(testWith: TestWith[BagUnpackerWorker[String, String], R]): R =
    withBagUnpackerWorker(
      queue = Queue("any", "any"),
      ingests = ingests,
      outgoing = outgoing,
      dstBucket = dstBucket
    ) {
      testWith(_)
    }
}
