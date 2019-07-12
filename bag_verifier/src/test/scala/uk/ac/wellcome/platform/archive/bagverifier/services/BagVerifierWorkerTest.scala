package uk.ac.wellcome.platform.archive.bagverifier.services

import io.circe.Encoder
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.{
  BagRootLocationPayload,
  EnrichedBagInformationPayload
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  FileEntry,
  S3BagLocationFixtures
}
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest

import scala.util.{Failure, Success, Try}

class BagVerifierWorkerTest
    extends FunSpec
    with Matchers
    with S3BagLocationFixtures
    with IngestUpdateAssertions
    with IntegrationPatience
    with BagVerifierFixtures
    with PayloadGenerators {

  it(
    "updates the ingest monitor and sends an outgoing notification if verification succeeds") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
      service =>
        withLocalS3Bucket { bucket =>
          withS3Bag(bucket) {
            case (bagRootLocation, _) =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              service.processMessage(payload) shouldBe a[Success[_]]

              assertTopicReceivesIngestEvents(
                payload.ingestId,
                ingests,
                expectedDescriptions = Seq(
                  "Verification started",
                  "Verification succeeded"
                )
              )

              outgoing.getMessages[EnrichedBagInformationPayload] shouldBe Seq(
                payload)
          }
        }
    }
  }

  describe("passes through the original payload, unmodified") {
    it("EnrichedBagInformationPayload") {
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      withBagVerifierWorker(ingests, outgoing) { service =>
        withLocalS3Bucket { bucket =>
          withS3Bag(bucket) {
            case (bagRootLocation, _) =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              service.processMessage(payload) shouldBe a[Success[_]]

              outgoing.getMessages[EnrichedBagInformationPayload] shouldBe Seq(
                payload)
          }
        }
      }
    }

    it("BagInformationPayload") {
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      withBagVerifierWorker(ingests, outgoing) { service =>
        withLocalS3Bucket { bucket =>
          withS3Bag(bucket) {
            case (bagRootLocation, _) =>
              val payload = createBagRootLocationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              service.processMessage(payload) shouldBe a[Success[_]]

              outgoing.getMessages[BagRootLocationPayload] shouldBe Seq(payload)
          }
        }
      }
    }
  }

  it("only updates the ingest monitor if verification fails") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
      service =>
        withLocalS3Bucket { bucket =>
          withS3Bag(bucket, createDataManifest = dataManifestWithWrongChecksum) {
            case (bagRootLocation, _) =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              service.processMessage(payload) shouldBe a[Success[_]]

              outgoing.messages shouldBe empty

              assertTopicReceivesIngestStatus(
                payload.ingestId,
                ingests,
                status = Ingest.Failed
              ) { events =>
                val description = events.map {
                  _.description
                }.head
                description should startWith("Verification failed")
              }
          }
        }
    }
  }

  it("only updates the ingest monitor if it cannot perform the verification") {
    def dontCreateTheDataManifest(
      dataFiles: List[(String, String)]): Option[FileEntry] = None

    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
      service =>
        withLocalS3Bucket { bucket =>
          withS3Bag(bucket, createDataManifest = dontCreateTheDataManifest) {
            case (bagRootLocation, _) =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              service.processMessage(payload) shouldBe a[Success[_]]

              outgoing.messages shouldBe empty

              assertTopicReceivesIngestStatus(
                payload.ingestId,
                ingests,
                status = Ingest.Failed
              ) { events =>
                val description = events.map {
                  _.description
                }.head
                description should startWith("Verification failed")
              }
          }
        }
    }
  }

  it("includes a specific error if the bag-info.txt is incorrect") {
    val ingests = new MemoryMessageSender()
    val outgoing = new MemoryMessageSender()

    val externalIdentifier = randomAlphanumeric
    val bagInfoExternalIdentifier = ExternalIdentifier(externalIdentifier + "_bag-info")
    val payloadExternalIdentifier = ExternalIdentifier(externalIdentifier + "_payload")

    withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
      service =>
        withLocalS3Bucket { bucket =>
          withS3Bag(bucket, externalIdentifier = bagInfoExternalIdentifier) {
            case (bagRootLocation, _) =>
              val payload = createEnrichedBagInformationPayloadWith(
                context = createPipelineContextWith(
                  externalIdentifier = payloadExternalIdentifier
                ),
                bagRootLocation = bagRootLocation
              )

              service.processMessage(payload) shouldBe a[Success[_]]

              assertTopicReceivesIngestStatus(
                payload.ingestId,
                ingests,
                status = Ingest.Failed
              ) { events =>
                val description = events.map {
                  _.description
                }.head
                description should startWith("Verification failed: External identifier in bag-info.txt does not match request")
              }
          }
        }
    }
  }

  it("sends a ingest update before it sends an outgoing message") {
    val ingests = new MemoryMessageSender()

    val outgoing = new MemoryMessageSender() {
      override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
        Failure(new Throwable("BOOM!"))
    }

    withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
      service =>
        withLocalS3Bucket { bucket =>
          withS3Bag(bucket) {
            case (bagRootLocation, _) =>
              val payload = createEnrichedBagInformationPayloadWith(
                bagRootLocation = bagRootLocation
              )

              service.processMessage(payload) shouldBe a[Failure[_]]

              assertTopicReceivesIngestEvent(payload.ingestId, ingests) {
                events =>
                  events.map {
                    _.description
                  } shouldBe List("Verification succeeded")
              }
          }
        }
    }
  }
}
