package uk.ac.wellcome.platform.archive.bagreplicator.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.ObjectLocationPayload
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest

class BagReplicatorWorkerTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagReplicatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators {

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
              withBag(ingestsBucket) {
                case (srcBagRootLocation, _) =>
                  val payload = createObjectLocationPayloadWith(
                    srcBagRootLocation
                  )

                  val future = service.processMessage(payload)

                  whenReady(future) { _ =>
                    val result =
                      notificationMessage[ObjectLocationPayload](outgoingTopic)
                    result.ingestId shouldBe payload.ingestId

                    val dstBagRootLocation = result.objectLocation

                    verifyBagCopied(
                      src = srcBagRootLocation,
                      dst = dstBagRootLocation
                    )

                    assertTopicReceivesIngestEvent(
                      payload.ingestId,
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
          val payload = createObjectLocationPayload

          val future = service.processMessage(payload)

          whenReady(future) { _ =>
            assertSnsReceivesNothing(outgoingTopic)

            assertTopicReceivesIngestStatus(
              payload.ingestId,
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
