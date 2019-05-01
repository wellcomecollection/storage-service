package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.nio.file.Paths

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.bagreplicator.fixtures.BagReplicatorFixtures
import uk.ac.wellcome.platform.archive.common.BagInformationPayload
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
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withBagReplicatorWorker(
              ingestTopic = ingestTopic,
              outgoingTopic = outgoingTopic,
              bucket = archiveBucket) { service =>
              withBag(ingestsBucket) {
                case (srcBagRootLocation, storageSpace) =>
                  val payload = createBagInformationPayloadWith(
                    bagRootLocation = srcBagRootLocation,
                    storageSpace = storageSpace
                  )

                  val future = service.processMessage(payload)

                  whenReady(future) { _ =>
                    val result =
                      notificationMessage[BagInformationPayload](outgoingTopic)
                    result.ingestId shouldBe payload.ingestId

                    val dstBagRootLocation = result.bagRootLocation

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

  describe("copies to the correct destination") {
    it("copies the bag to the configured bucket") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          withBagReplicatorWorker(archiveBucket) { worker =>
            withBag(ingestsBucket) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                val future = worker.processMessage(payload)

                whenReady(future) { result =>
                  val destination = result.summary.get.destination
                  destination.namespace shouldBe archiveBucket.name
                }
            }
          }
        }
      }
    }

    it("constructs the correct key") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          val config = createReplicatorDestinationConfigWith(archiveBucket)
          withBagReplicatorWorker(config) { worker =>
            val bagInfo = createBagInfo
            withBag(ingestsBucket, bagInfo = bagInfo) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                val future = worker.processMessage(payload)

                whenReady(future) { result =>
                  val destination = result.summary.get.destination
                  val expectedKey =
                    Paths
                      .get(
                        config.rootPath.get,
                        payload.storageSpace.underlying,
                        payload.externalIdentifier.toString,
                        s"v${payload.version}"
                      )
                      .toString
                  destination.key shouldBe expectedKey
                }
            }
          }
        }
      }
    }

    it("key ends with the external identifier and version of the bag") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          withBagReplicatorWorker(archiveBucket) { worker =>
            withBag(ingestsBucket) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation,
                  version = 3
                )

                val future = worker.processMessage(payload)

                whenReady(future) { result =>
                  val destination = result.summary.get.destination
                  destination.key should endWith(
                    s"/${payload.externalIdentifier.toString}/v3")
                }
            }
          }
        }
      }
    }

    it("prefixes the key with the storage space if no root path is set") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          val config = createReplicatorDestinationConfigWith(
            bucket = archiveBucket,
            rootPath = None
          )
          withBagReplicatorWorker(config) { worker =>
            withBag(ingestsBucket) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                val future = worker.processMessage(payload)

                whenReady(future) { result =>
                  val destination = result.summary.get.destination
                  destination.key should startWith(
                    payload.storageSpace.underlying)
                }
            }
          }
        }
      }
    }

    it("prefixes the key with the root path if set") {
      withLocalS3Bucket { ingestsBucket =>
        withLocalS3Bucket { archiveBucket =>
          val config = createReplicatorDestinationConfigWith(
            bucket = archiveBucket,
            rootPath = Some("rootprefix")
          )
          withBagReplicatorWorker(config) { worker =>
            withBag(ingestsBucket) {
              case (bagRootLocation, _) =>
                val payload = createBagInformationPayloadWith(
                  bagRootLocation = bagRootLocation
                )

                val future = worker.processMessage(payload)

                whenReady(future) { result =>
                  val destination = result.summary.get.destination
                  destination.key should startWith("rootprefix/")
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
          val payload = createBagInformationPayload

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
