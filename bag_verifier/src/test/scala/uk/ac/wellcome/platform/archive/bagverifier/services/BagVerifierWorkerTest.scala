package uk.ac.wellcome.platform.archive.bagverifier.services

import io.circe.Encoder
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
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
    with ScalaFutures
    with BagLocationFixtures
    with IngestUpdateAssertions
    with IntegrationPatience
    with BagVerifierFixtures
    with PayloadGenerators {

  it(
    "updates the ingests app and sends an outgoing notification if verification succeeds") {
    val ingests = createMessageSender
    val outgoing = createMessageSender

    withBagVerifierWorker(ingests, outgoing) { service =>
      withLocalS3Bucket { bucket =>
        withBag(storageBackend, namespace = bucket.name) {
          case (bagRootLocation, _) =>
            val payload = createBagInformationPayloadWith(
              bagRootLocation = bagRootLocation
            )

            service.processMessage(payload) shouldBe a[Success[_]]

            assertReceivesIngestEvents(ingests)(
              payload.ingestId,
              expectedDescriptions = Seq(
                "Verification started",
                "Verification succeeded"
              )
            )

            outgoing.getMessages[BagInformationPayload]() shouldBe Seq(payload)
        }
      }
    }
  }

  it("only updates the ingest monitor if verification fails") {
    val ingests = createMessageSender
    val outgoing = createMessageSender

    withBagVerifierWorker(ingests, outgoing) { service =>
      withLocalS3Bucket { bucket =>
        withBag(
          storageBackend,
          namespace = bucket.name,
          createDataManifest = dataManifestWithWrongChecksum) {
          case (bagRootLocation, _) =>
            val payload = createBagInformationPayloadWith(
              bagRootLocation = bagRootLocation
            )

            service.processMessage(payload) shouldBe a[Success[_]]

            outgoing.messages shouldBe empty

            assertReceivesIngestStatus(ingests)(
              ingestId = payload.ingestId,
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

    val ingests = createMessageSender
    val outgoing = createMessageSender

    withBagVerifierWorker(ingests, outgoing) { service =>
      withLocalS3Bucket { bucket =>
        withBag(
          storageBackend,
          namespace = bucket.name,
          createDataManifest = dontCreateTheDataManifest) {
          case (bagRootLocation, _) =>
            val payload = createBagInformationPayloadWith(
              bagRootLocation = bagRootLocation
            )

            service.processMessage(payload) shouldBe a[Success[_]]

            outgoing.messages shouldBe empty

            assertReceivesIngestStatus(ingests)(
              ingestId = payload.ingestId,
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
    val ingests = createMessageSender

    val outgoing = new MemoryMessageSender(
      destination = randomAlphanumeric(),
      subject = randomAlphanumeric()
    ) {
      override def sendT[T](t: T)(implicit encoder: Encoder[T]): Try[Unit] =
        Failure(new Throwable("BOOM!"))
    }

    withBagVerifierWorker(ingests, outgoing) { service =>
      withLocalS3Bucket { bucket =>
        withBag(storageBackend, namespace = bucket.name) {
          case (bagRootLocation, _) =>
            val payload = createBagInformationPayloadWith(
              bagRootLocation = bagRootLocation
            )

            service.processMessage(payload) shouldBe a[Failure[_]]

            assertReceivesIngestEvent(ingests)(payload.ingestId) { events =>
              events.map {
                _.description
              } shouldBe List("Verification succeeded")
            }
        }
      }
    }
  }
}
