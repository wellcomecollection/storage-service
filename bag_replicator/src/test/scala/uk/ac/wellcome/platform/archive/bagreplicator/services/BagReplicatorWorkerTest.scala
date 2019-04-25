package uk.ac.wellcome.platform.archive.bagreplicator.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagLocation,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  BagRequest,
  Ingest
}

class BagReplicatorWorkerTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagReplicatorFixtures
    with BagRequestGenerators
    with IngestUpdateAssertions {

  it("replicates a bag successfully and updates both topics") {
    withLocalS3Bucket { ingestsBucket =>
      withLocalS3Bucket { archiveBucket =>
        val destination = createReplicatorDestinationConfigWith(archiveBucket)
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withBagReplicatorWorker(
              ingestTopic = ingestTopic,
              outgoingTopic = outgoingTopic,
              config = destination) { service =>
              withBag(ingestsBucket) { srcBagLocation =>
                val bagRequest = createBagRequestWith(srcBagLocation)

                val future = service.processMessage(bagRequest)

                whenReady(future) { _ =>
                  val result = notificationMessage[BagRequest](outgoingTopic)
                  result.ingestId shouldBe bagRequest.ingestId

                  val dstBagLocation = result.bagLocation

                  verifyBagCopied(
                    src = srcBagLocation,
                    dst = dstBagLocation
                  )

                  assertTopicReceivesIngestEvent(
                    bagRequest.ingestId,
                    ingestTopic) { events =>
                    events should have size 1
                    events.head.description shouldBe "Replicating succeeded"
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  it("sends a failed IngestUpdate if replication fails") {
    withLocalSnsTopic { ingestTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withBagReplicatorWorker(
          ingestTopic = ingestTopic,
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

            assertTopicReceivesIngestStatus(
              bagRequest.ingestId,
              ingestTopic = ingestTopic,
              status = Ingest.Failed) { events =>
              events should have size 1
              events.head.description shouldBe "Replicating failed"
            }
          }
        }
      }
    }
  }
}
