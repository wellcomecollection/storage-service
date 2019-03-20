package uk.ac.wellcome.platform.archive.bagunpacker

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.{
  CompressFixture,
  TestArchive,
  WorkerServiceFixture
}
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagLocation,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  BagRequest,
  Ingest,
  UnpackBagRequest
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

class UnpackerFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with RandomThings
    with WorkerServiceFixture
    with IntegrationPatience
    with CompressFixture
    with IngestUpdateAssertions {

  it("receives and processes a notification") {
    withApp {
      case (_, srcBucket, queue, ingestTopic, outgoingTopic) =>
        val (archiveFile, filesInArchive, entries) =
          createTgzArchiveWithRandomFiles()
        withArchive(srcBucket, archiveFile) { archiveLocation =>
          withBagNotification(
            queue,
            srcBucket,
            randomUUID,
            TestArchive(archiveFile, filesInArchive, entries, archiveLocation)
          ) { unpackBagRequest =>
            eventually {

              val expectedNotification = BagRequest(
                requestId = unpackBagRequest.requestId,
                bagLocation = BagLocation(
                  storageNamespace = srcBucket.name,
                  storagePrefix = None,
                  storageSpace = unpackBagRequest.storageSpace,
                  bagPath = BagPath(unpackBagRequest.requestId.toString)
                )
              )

              assertSnsReceivesOnly[BagRequest](
                expectedNotification,
                outgoingTopic
              )

              topicReceivesIngestEvent(
                requestId = unpackBagRequest.requestId,
                ingestTopic = ingestTopic
              ) { events =>
                events.map {
                  _.description
                } shouldBe List(
                  "Unpacker succeeded"
                )
              }
            }
          }
        }
    }
  }

  it("sends a failed Ingest update if it cannot read the bag") {
    withApp {
      case (service, _, _, ingestTopic, outgoingTopic) =>
        val unpackBagRequest = UnpackBagRequest(
          requestId = randomUUID,
          sourceLocation = createObjectLocation,
          storageSpace = StorageSpace(randomAlphanumeric())
        )

        val future = service.processMessage(unpackBagRequest)

        whenReady(future) { _ =>
          assertSnsReceivesNothing(outgoingTopic)

          topicReceivesIngestStatus(
            requestId = unpackBagRequest.requestId,
            ingestTopic = ingestTopic,
            status = Ingest.Failed
          ) { events =>
            events.map { _.description } shouldBe List("Unpacker failed")
          }
        }
    }
  }
}
