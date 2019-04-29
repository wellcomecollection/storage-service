package uk.ac.wellcome.platform.storage.bagauditor.services
import java.nio.file.Paths

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
      withBag(bucket, bagInfo = bagInfo) { bagLocation =>
        val future = bagAuditor.getAuditSummary(
          unpackLocation = bagLocation.objectLocation,
          storageSpace = bagLocation.storageSpace
        )

        whenReady(future) { result =>
          val auditSummary = result.summary
          val auditInformation = auditSummary.auditInformation

          auditInformation.bagRootLocation shouldBe bagLocation.objectLocation
          auditInformation.externalIdentifier shouldBe bagInfo.externalIdentifier
        }
      }
    }
  }

  it("errors if it cannot find the bag root") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, bagRootDirectory = Some("1/2/3")) { bagLocation =>
        val future = bagAuditor.getAuditSummary(
          unpackLocation = createObjectLocationWith(bucket, key = "1/"),
          storageSpace = bagLocation.storageSpace
        )

        whenReady(future) { result =>
          result shouldBe a[IngestFailed[_]]
        }
      }
    }
  }

  it("errors if it cannot find the bag identifier") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { bagLocation =>
        s3Client.deleteObject(
          bagLocation.objectLocation.namespace,
          Paths.get(bagLocation.objectLocation.key, "bag-info.txt").toString
        )

        val future = bagAuditor.getAuditSummary(
          unpackLocation = bagLocation.objectLocation,
          storageSpace = bagLocation.storageSpace
        )

        whenReady(future) { result =>
          result shouldBe a[IngestFailed[_]]
        }
      }
    }
  }

}
