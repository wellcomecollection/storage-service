package uk.ac.wellcome.platform.storage.bagauditor

import org.scalatest.FunSpec
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagLocation, BagPath}
import uk.ac.wellcome.platform.archive.common.generators.BagRequestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.BagRequest
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.BagAuditorFixtures
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3.Bucket

class BagAuditorFeatureTest
    extends FunSpec
    with BagAuditorFixtures
    with BagRequestGenerators
    with IngestUpdateAssertions {

  it("detects a bag in the root of the bagLocation") {
    withLocalS3Bucket { bucket =>
      createObjectsWith(
        bucket,
        "bag123/bag-info.txt",
        "bag123/data/1.jpg",
        "bag123/data/2.jpg"
      )

      val objectLocation = createObjectLocationWith(bucket, "bag123")
      val bagRequest = createBagRequestWith(objectLocation)

      withLocalSqsQueue { queue =>
        withLocalSnsTopic { ingestTopic =>
          withLocalSnsTopic { outgoingTopic =>
            withAuditorWorker(queue, ingestTopic, outgoingTopic) { _ =>
              sendNotificationToSQS(queue, bagRequest)

              eventually {
                assertQueueEmpty(queue)

                val result = notificationMessage[BagRequest](outgoingTopic)
                result.requestId shouldBe bagRequest

                assertTopicReceivesIngestEvent(
                  bagRequest.requestId,
                  ingestTopic) { events =>
                  events should have size 1
                  events.head.description shouldBe "Locating bag root succeeded"
                }
              }
            }
          }
        }
      }
    }
  }

  def createBagRequestWith(objectLocation: ObjectLocation): BagRequest =
    createBagRequestWith(
      bagLocation = BagLocation(
        storageNamespace = objectLocation.namespace,
        storagePrefix = None,
        storageSpace = StorageSpace(""),
        bagPath = BagPath(objectLocation.key)
      )
    )

  def createObjectsWith(bucket: Bucket, keys: String*): Unit =
    keys.foreach { k =>
      s3Client.putObject(bucket.name, k, "example object")
    }
}
