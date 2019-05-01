package uk.ac.wellcome.platform.archive.bag_register.services

import java.time.Instant

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.bag_register.fixtures.BagRegisterFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  PayloadGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  Ingest,
  StorageLocation
}
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class BagRegisterWorkerTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagInfoGenerators
    with BagLocationFixtures
    with IngestUpdateAssertions
    with BagRegisterFixtures
    with PayloadGenerators {

  it("sends a successful IngestUpdate upon registration") {
    withBagRegisterWorker {
      case (service, table, bucket, ingestTopic, _, _) =>
        val createdAfterDate = Instant.now()
        val bagInfo = createBagInfo

        withBag(bucket, bagInfo = bagInfo) {
          case (bagRootLocation, storageSpace) =>
            val payload = createBagInformationPayloadWith(
              bagRootLocation = bagRootLocation,
              storageSpace = storageSpace
            )

            val bagId = BagId(
              space = storageSpace,
              externalIdentifier = bagInfo.externalIdentifier
            )

            val future = service.processMessage(payload)

            whenReady(future) { _ =>
              val storageManifest = getStorageManifest(table, id = bagId)

              storageManifest.space shouldBe bagId.space
              storageManifest.info shouldBe bagInfo
              storageManifest.manifest.files should have size 1

              storageManifest.locations shouldBe List(
                StorageLocation(
                  provider = InfrequentAccessStorageProvider,
                  location = bagRootLocation
                )
              )

              storageManifest.createdDate.isAfter(createdAfterDate) shouldBe true

              assertTopicReceivesIngestStatus(
                ingestId = payload.ingestId,
                ingestTopic = ingestTopic,
                status = Ingest.Completed,
                expectedBag = Some(bagId)) { events =>
                events.size should be >= 1
                events.head.description shouldBe "Register succeeded (completed)"
              }
            }
        }
    }
  }

  it("sends a failed IngestUpdate if storing fails") {
    withBagRegisterWorkerAndBucket(Bucket("does-not-exist")) {
      case (service, _, bucket, ingestTopic, _, _) =>
        val bagInfo = createBagInfo

        withBag(bucket, bagInfo = bagInfo) {
          case (bagRootLocation, storageSpace) =>
            val payload = createBagInformationPayloadWith(
              bagRootLocation = bagRootLocation,
              storageSpace = storageSpace
            )

            val bagId = BagId(
              space = storageSpace,
              externalIdentifier = bagInfo.externalIdentifier
            )

            val future = service.processMessage(payload)

            whenReady(future) { _ =>
              assertTopicReceivesIngestStatus(
                ingestId = payload.ingestId,
                ingestTopic = ingestTopic,
                status = Ingest.Failed,
                expectedBag = Some(bagId)) { events =>
                events.size should be >= 1
                events.head.description shouldBe "Register failed"
              }
            }
        }
    }
  }
}
