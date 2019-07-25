package uk.ac.wellcome.platform.archive.bag_register

import java.time.Instant

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators

class BagRegisterFeatureTest
    extends FunSpec
    with Matchers
    with BagRegisterFixtures
    with PayloadGenerators {

  it("sends an update if it registers a bag") {
    withBagRegisterWorker {
      case (_, storageManifestDao, ingests, _, queuePair) =>
        val createdAfterDate = Instant.now()
        val space = createStorageSpace
        val version = randomInt(1, 15)
        val dataFileCount = randomInt(1, 15)
        val externalIdentifier = createExternalIdentifier

        val bagId = BagId(
          space = space,
          externalIdentifier = externalIdentifier
        )

        withLocalS3Bucket { bucket =>
          withBag(bucket, externalIdentifier, space, version, dataFileCount = dataFileCount) {
            case (bagRootLocation, bagInfo) =>
              val payload = createEnrichedBagInformationPayloadWith(
                context = createPipelineContextWith(
                  storageSpace = space
                ),
                bagRootLocation = bagRootLocation,
                version = version
              )

              sendNotificationToSQS(queuePair.queue, payload)

              eventually {
                val storageManifest =
                  storageManifestDao.getLatest(bagId).right.value

                storageManifest.space shouldBe bagId.space
                storageManifest.info shouldBe bagInfo
                storageManifest.manifest.files should have size dataFileCount

                storageManifest.locations should have size 1

                storageManifest.createdDate.isAfter(createdAfterDate) shouldBe true

                assertBagRegisterSucceeded(
                  ingestId = payload.ingestId,
                  ingests = ingests
                )

                assertQueueEmpty(queuePair.queue)
              }
          }
        }
    }
  }

  it("sends a failed update and discards the work on error") {
    withBagRegisterWorker {
      case (_, _, ingests, _, queuePair) =>
        val payload = createEnrichedBagInformationPayload

        sendNotificationToSQS(queuePair.queue, payload)

        eventually {
          assertBagRegisterFailed(
            ingestId = payload.ingestId,
            ingests = ingests
          )
        }

        assertQueueEmpty(queuePair.queue)
        assertQueueEmpty(queuePair.dlq)
    }
  }
}
