package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.bagverifier.fixtures.BagVerifierFixtures
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  FileEntry
}
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions

class VerifierWorkerTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures
    with BagRequestGenerators
    with IngestUpdateAssertions
    with IntegrationPatience
    with BagVerifierFixtures {

  it(
    "updates the ingest monitor and sends an outgoing notification if verification succeeds") {
    withLocalSnsTopic { ingestTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withBagVerifierWorker(ingestTopic, outgoingTopic) { service =>
          withLocalS3Bucket { bucket =>
            withBag(bucket) { bagLocation =>
              val bagRequest = createBagRequestWith(bagLocation)

              val future = service.processMessage(bagRequest)

              whenReady(future) { _ =>
                eventually {
                  assertTopicReceivesIngestEvent(
                    requestId = bagRequest.requestId,
                    ingestTopic = ingestTopic
                  ) { events =>
                    events.map {
                      _.description
                    } shouldBe List(
                      "Verification succeeded"
                    )
                  }

                  assertSnsReceivesOnly(bagRequest, topic = outgoingTopic)
                }
              }
            }
          }
        }
      }
    }
  }

  it("only updates the ingest monitor if verification fails") {
    withLocalSnsTopic { ingestTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withBagVerifierWorker(ingestTopic, outgoingTopic) { service =>
          withLocalS3Bucket { bucket =>
            withBag(bucket, createDataManifest = dataManifestWithWrongChecksum) {
              bagLocation =>
                val bagRequest = createBagRequestWith(bagLocation)

                val future = service.processMessage(bagRequest)

                whenReady(future) { _ =>
                  assertSnsReceivesNothing(outgoingTopic)

                  assertTopicReceivesIngestStatus(
                    requestId = bagRequest.requestId,
                    ingestTopic = ingestTopic,
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
      }
    }
  }

  it("only updates the ingest monitor if it cannot perform the verification") {
    def dontCreateTheDataManifest(
      dataFiles: List[(String, String)]): Option[FileEntry] = None

    withLocalSnsTopic { ingestTopic =>
      withLocalSnsTopic { outgoingTopic =>
        withBagVerifierWorker(ingestTopic, outgoingTopic) { service =>
          withLocalS3Bucket { bucket =>
            withBag(bucket, createDataManifest = dontCreateTheDataManifest) {
              bagLocation =>
                val bagRequest = createBagRequestWith(bagLocation)

                val future = service.processMessage(bagRequest)

                whenReady(future) { _ =>
                  eventually {

                    assertSnsReceivesNothing(outgoingTopic)

                    assertTopicReceivesIngestStatus(
                      requestId = bagRequest.requestId,
                      ingestTopic = ingestTopic,
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
        }
      }
    }
  }

  it("sends a ingest update before it sends an outgoing message") {
    withLocalSnsTopic { ingestTopic =>
      withBagVerifierWorker(ingestTopic, Topic("no-such-outgoing")) { service =>
        withLocalS3Bucket { bucket =>
          withBag(bucket) { bagLocation =>
            val bagRequest = createBagRequestWith(bagLocation)

            val future = service.processMessage(bagRequest)

            whenReady(future.failed) { _ =>
              assertTopicReceivesIngestEvent(
                requestId = bagRequest.requestId,
                ingestTopic = ingestTopic
              ) { events =>
                events.map {
                  _.description
                } shouldBe List("Verification succeeded")
              }
            }
          }
        }
      }
    }
  }
}
