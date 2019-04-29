package uk.ac.wellcome.platform.storage.bagauditor.services
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.storage.models.IngestFailed

class BagAuditorTest extends FunSpec with Matchers with BagLocationFixtures {
  val bagAuditor = new BagAuditor()

  it("gets the audit information for a valid bag") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { bagLocation =>
        val result = bagAuditor.getAuditSummary(
          unpackLocation = bagLocation.objectLocation,
          storageSpace = bagLocation.storageSpace
        )

        val auditSummary = result.get.summary
        val auditInformation = auditSummary.auditInformation

        auditInformation.bagRoot shouldBe bagLocation.objectLocation
      }
    }
  }

  it("errors if it cannot find the bag root") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, bagRootDirectory = Some("1/2/3")) { bagLocation =>
        val result = bagAuditor.getAuditSummary(
          unpackLocation = createObjectLocationWith(bucket, key = "1/"),
          storageSpace = bagLocation.storageSpace
        )

        result.get shouldBe a[IngestFailed[_]]
      }
    }
  }
}
