package weco.storage_service.bag_unpacker.services

import java.nio.file.Paths
import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.{LocalResources, TestWith}
import weco.json.JsonUtil._
import weco.messaging.memory.MemoryMessageSender
import weco.storage_service.bag_unpacker.fixtures.BagUnpackerFixtures
import weco.storage_service.bag_unpacker.fixtures.s3.S3CompressFixture
import weco.storage_service.UnpackedBagLocationPayload
import weco.storage_service.generators.PayloadGenerators
import weco.storage_service.ingests.fixtures.IngestUpdateAssertions
import weco.storage_service.storage.models.{IngestFailed, IngestStepSucceeded}
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.s3.S3ObjectLocationPrefix

class BagUnpackerWorkerTest
    extends AnyFunSpec
    with Matchers
    with BagUnpackerFixtures
    with S3CompressFixture
    with IngestUpdateAssertions
    with PayloadGenerators
    with LocalResources
    with TryValues {

  it("processes a message") {
    val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()

    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withStreamStore { implicit streamStore =>
      withLocalS3Bucket { srcBucket =>
        withLocalS3Bucket { dstBucket =>
          withArchive(srcBucket, archiveFile) { archiveLocation =>
            val payload = createSourceLocationPayloadWith(archiveLocation)

            val result =
              withWorker(ingests, outgoing, dstBucket) { worker =>
                worker.processMessage(payload)
              }

            result.success.value shouldBe a[IngestStepSucceeded[_]]

            val expectedPayload = UnpackedBagLocationPayload(
              context = payload.context,
              unpackedBagLocation = S3ObjectLocationPrefix(
                bucket = dstBucket.name,
                keyPrefix = Paths
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

            assertTopicReceivesIngestUpdates(ingests) { ingestUpdates =>
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
      val location = createS3ObjectLocationWith(srcBucket)

      putStream(location)

      val payload = createSourceLocationPayloadWith(location)

      val result =
        withWorker(ingests, outgoing) { worker =>
          worker.processMessage(payload)
        }

      result.success.value shouldBe a[IngestFailed[_]]
      val failure = result.success.value.asInstanceOf[IngestFailed[_]]

      val message =
        s"Error trying to unpack the archive at $location - is it the correct format?"

      failure.maybeUserFacingMessage.get shouldBe message

      assertTopicReceivesIngestUpdates(ingests) { ingestUpdates =>
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
      val location = createS3ObjectLocationWith(srcBucket)

      putString(location, readResource("/crockery.7z"))

      val payload = createSourceLocationPayloadWith(location)

      val result =
        withWorker(ingests, outgoing) { worker =>
          worker.processMessage(payload)
        }

      result.success.value shouldBe a[IngestFailed[_]]
      val failure = result.success.value.asInstanceOf[IngestFailed[_]]

      val message =
        s"Error trying to unpack the archive at $location - is it the correct format?"

      failure.maybeUserFacingMessage.get shouldBe message

      assertTopicReceivesIngestUpdates(ingests) { ingestUpdates =>
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
      ingests = ingests,
      outgoing = outgoing,
      dstBucket = dstBucket,
      stepName = "Unpacker"
    ) {
      testWith(_)
    }
}
