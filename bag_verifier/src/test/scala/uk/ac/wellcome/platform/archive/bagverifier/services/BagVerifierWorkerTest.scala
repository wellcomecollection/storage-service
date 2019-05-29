package uk.ac.wellcome.platform.archive.bagverifier.services

import io.circe.Encoder
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  FileEntry
}
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest

import scala.util.{Failure, Success, Try}

class BagVerifierWorkerTest
    extends FunSpec
    with Matchers
    with BagLocationFixtures
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
          withBag(bucket) {
            case (bagRootLocation, _) =>
              val payload = createBagInformationPayloadWith(
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

              outgoing.getMessages[BagInformationPayload] shouldBe Seq(payload)
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
          withBag(bucket, createDataManifest = dataManifestWithWrongChecksum) {
            case (bagRootLocation, _) =>
              val payload = createBagInformationPayloadWith(
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
          withBag(bucket, createDataManifest = dontCreateTheDataManifest) {
            case (bagRootLocation, _) =>
              val payload = createBagInformationPayloadWith(
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

  it("sends a ingest update before it sends an outgoing message") {
    val ingests = new MemoryMessageSender()

    val outgoing = new MemoryMessageSender() {
      override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
        Failure(new Throwable("BOOM!"))
    }

    withBagVerifierWorker(ingests, outgoing, stepName = "verification") {
      service =>
        withLocalS3Bucket { bucket =>
          withBag(bucket) {
            case (bagRootLocation, _) =>
              val payload = createBagInformationPayloadWith(
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
