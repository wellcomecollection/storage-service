package uk.ac.wellcome.platform.archive.bagunpacker.services

import java.nio.file.Paths

import com.amazonaws.services.s3.model.ObjectMetadata
import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.{
  BagUnpackerFixtures,
  CompressFixture
}
import uk.ac.wellcome.platform.archive.common.UnpackedBagLocationPayload
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepSucceeded
}
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket

class BagUnpackerWorkerTest
    extends FunSpec
    with Matchers
    with BagUnpackerFixtures
    with CompressFixture[Bucket]
    with IngestUpdateAssertions
    with PayloadGenerators
    with TryValues {

  it("processes a message") {
    val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()

    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withStreamStore { implicit streamStore =>
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          withArchive(srcBucket, archiveFile) { archiveLocation =>
            val payload =
              createSourceLocationPayloadWith(archiveLocation)

            val result =
              withWorker(ingests, outgoing, dstBucket) { worker =>
                worker.processMessage(payload)
              }

            result.success.value shouldBe a[IngestStepSucceeded[_]]

            val expectedPayload = UnpackedBagLocationPayload(
              context = payload.context,
              unpackedBagLocation = ObjectLocationPrefix(
                namespace = dstBucket.name,
                path = Paths
                  .get(
                    payload.storageSpace.toString,
                    payload.ingestId.toString
                  )
                  .toString
              )
            )

            outgoing.getMessages[UnpackedBagLocationPayload] shouldBe Seq(
              expectedPayload
            )

            assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
              ingestUpdates =>
                val eventDescriptions: Seq[String] =
                  ingestUpdates
                    .flatMap { _.events }
                    .map { _.description }
                    .distinct

                eventDescriptions should have size 2

                eventDescriptions(0) shouldBe "Unpacker started"
                eventDescriptions(1) should fullyMatch regex """Unpacker succeeded - Unpacked \d+ KB from \d+ files"""
            }
          }
        }
      }
    }
  }

  it("reports a compressor error to the user") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { srcBucket =>
      val location = createObjectLocationWith(srcBucket)

      s3Client.putObject(
        location.namespace,
        location.path,
        "hello world"
      )

      val payload =
        createSourceLocationPayloadWith(location)

      val result =
        withWorker(ingests, outgoing) { worker =>
          worker.processMessage(payload)
        }

      result.success.value shouldBe a[IngestFailed[_]]
      val failure = result.success.value.asInstanceOf[IngestFailed[_]]

      val message =
        s"Error trying to unpack the archive at s3://$location - is it the correct format?"

      failure.maybeUserFacingMessage.get shouldBe message

      assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
        ingestUpdates =>
          val eventDescriptions: Seq[String] =
            ingestUpdates
              .flatMap { _.events }
              .map { _.description }
              .distinct

          eventDescriptions shouldBe Seq(
            "Unpacker started",
            s"Unpacker failed - $message"
          )
      }
    }
  }

  it("reports an error if passed a 7z file") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withLocalS3Bucket { srcBucket =>
      val location = createObjectLocationWith(srcBucket)

      val inputStream = getClass.getResourceAsStream("/crockery.7z")

      s3Client.putObject(
        location.namespace,
        location.path,
        inputStream,
        new ObjectMetadata()
      )

      val payload =
        createSourceLocationPayloadWith(location)

      val result =
        withWorker(ingests, outgoing) { worker =>
          worker.processMessage(payload)
        }

      result.success.value shouldBe a[IngestFailed[_]]
      val failure = result.success.value.asInstanceOf[IngestFailed[_]]

      val message =
        s"Error trying to unpack the archive at s3://$location - is it the correct format?"

      failure.maybeUserFacingMessage.get shouldBe message

      assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
        ingestUpdates =>
          val eventDescriptions: Seq[String] =
            ingestUpdates
              .flatMap { _.events }
              .map { _.description }
              .distinct

          eventDescriptions shouldBe Seq(
            "Unpacker started",
            s"Unpacker failed - $message"
          )
      }
    }
  }

  def withWorker[R](
    ingests: MemoryMessageSender,
    outgoing: MemoryMessageSender,
    dstBucket: Bucket = createBucket
  )(testWith: TestWith[BagUnpackerWorker[String, String], R]): R =
    withBagUnpackerWorker(
      queue = Queue("any", "any"),
      ingests = ingests,
      outgoing = outgoing,
      dstBucket = dstBucket,
      stepName = "Unpacker"
    ) {
      testWith(_)
    }
}
