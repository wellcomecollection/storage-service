package uk.ac.wellcome.platform.storage.bagauditor.services

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.storage.models.IngestFailed

import scala.concurrent.ExecutionContext.Implicits.global

class BagAuditorTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagLocationFixtures {
  val bagAuditor = new BagAuditor()

  it("gets the audit information for a valid bag") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo) {
        case (bagRootLocation, storageSpace) =>
          val future = bagAuditor.getAuditSummary(
            unpackLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          whenReady(future) { result =>
            val auditSummary = result.summary
            val auditInformation = auditSummary.auditInformation

            auditInformation.bagRootLocation shouldBe bagRootLocation
            auditInformation.externalIdentifier shouldBe bagInfo.externalIdentifier
            auditInformation.version shouldBe 1
          }
      }
    }
  }

  it("errors if it cannot find the bag root") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, bagRootDirectory = Some("1/2/3")) {
        case (_, storageSpace) =>
          val future = bagAuditor.getAuditSummary(
            unpackLocation = createObjectLocationWith(bucket, key = "1/"),
            storageSpace = storageSpace
          )

          whenReady(future) { result =>
            result shouldBe a[IngestFailed[_]]
          }
      }
    }
  }

  it("errors if it cannot find the bag identifier") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) {
        case (bagRootLocation, storageSpace) =>
          val bagInfoLocation = bagRootLocation.join("bag-info.txt")
          s3Client.deleteObject(
            bagInfoLocation.namespace,
            bagInfoLocation.key
          )

          val future = bagAuditor.getAuditSummary(
            unpackLocation = bagRootLocation,
            storageSpace = storageSpace
          )

          whenReady(future) { result =>
            result shouldBe a[IngestFailed[_]]
          }
      }
    }
  }
}
