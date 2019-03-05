package uk.ac.wellcome.platform.archive.bagreplicator_unpack_to_archive.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator_unpack_to_archive.fixtures.{BagReplicatorFixtures, WorkerServiceFixture}
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.models.BagRequest
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.progress.ProgressUpdateAssertions
import uk.ac.wellcome.platform.archive.common.progress.models.Progress

class BagReplicatorWorkerServiceTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagReplicatorFixtures
    with BagRequestGenerators
    with ProgressUpdateAssertions
    with WorkerServiceFixture {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { ingestsBucket =>
      withLocalS3Bucket { archiveBucket =>
        val destination = createReplicatorDestinationConfigWith(archiveBucket)
        withLocalSnsTopic { progressTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withWorkerService(
              progressTopic = progressTopic,
              outgoingTopic = outgoingTopic,
              destination = destination) { service =>
              withBag(ingestsBucket) { srcBagLocation =>
                val bagRequest = createBagRequestWith(srcBagLocation)

                val future = service.processMessage(bagRequest)

                whenReady(future) { _ =>
                  val result = notificationMessage[BagRequest](outgoingTopic)
                  result.archiveRequestId shouldBe bagRequest.archiveRequestId

                  val dstBagLocation = result.bagLocation

                  verifyBagCopied(
                    src = srcBagLocation,
                    dst = dstBagLocation
                  )

                  assertTopicReceivesProgressEventUpdate(
                    bagRequest.archiveRequestId,
                    progressTopic) { events =>
                    events should have size 1
                    events.head.description shouldBe "Bag successfully copied from ingest location"
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  it("sends a failed ProgressUpdate if the bag fails to replicate") {
    withLocalSnsTopic { progressTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withWorkerService(
          progressTopic = progressTopic,
          outgoingTopic = outgoingTopic) { service =>
          val srcBagLocation = BagLocation(
            storageNamespace = "does-not-exist",
            storagePrefix = Some("does/not/"),
            storageSpace = createStorageSpace,
            bagPath = BagPath("exist.txt")
          )

          val bagRequest = createBagRequestWith(srcBagLocation)

          val future = service.processMessage(bagRequest)

          whenReady(future) { _ =>
            assertSnsReceivesNothing(outgoingTopic)

            assertTopicReceivesProgressStatusUpdate(
              bagRequest.archiveRequestId,
              progressTopic = progressTopic,
              status = Progress.Failed) { events =>
              events should have size 1
              events.head.description shouldBe "Failed to copy bag from ingest location"
            }
          }
        }
      }
    }
  }
}
