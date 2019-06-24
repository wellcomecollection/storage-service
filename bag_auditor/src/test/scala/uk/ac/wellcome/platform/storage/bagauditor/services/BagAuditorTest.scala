package uk.ac.wellcome.platform.storage.bagauditor.services

import java.time.Instant

import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CreateIngestType,
  UpdateIngestType
}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestFailed
import uk.ac.wellcome.platform.storage.bagauditor.fixtures.BagAuditorFixtures
import uk.ac.wellcome.platform.storage.bagauditor.models.{
  AuditFailureSummary,
  AuditSuccessSummary
}

class BagAuditorTest
    extends FunSpec
    with Matchers
    with TryValues
    with BagLocationFixtures
    with BagAuditorFixtures {

  it("gets the audit information for a valid bag") {
    withLocalS3Bucket { bucket =>
      val bagInfo = createBagInfo
      withBag(bucket, bagInfo = bagInfo) {
        case (bagRootLocation, storageSpace) =>
          withBagAuditor { bagAuditor =>
            val maybeAudit = bagAuditor.getAuditSummary(
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              root = bagRootLocation,
              storageSpace = storageSpace
            )

            val result = maybeAudit.success.get
            val summary = result.summary
              .asInstanceOf[AuditSuccessSummary]

            summary.audit.externalIdentifier shouldBe bagInfo.externalIdentifier
            summary.audit.version shouldBe 1
          }
      }
    }
  }

  it("errors if it cannot find the bag") {
    withBagAuditor { bagAuditor =>
      val maybeAudit = bagAuditor.getAuditSummary(
        ingestId = createIngestID,
        ingestDate = Instant.now,
        ingestType = CreateIngestType,
        root = createObjectLocation,
        storageSpace = createStorageSpace
      )

      val result = maybeAudit.success.get

      result shouldBe a[IngestFailed[_]]
      result.summary shouldBe a[AuditFailureSummary]
    }
  }

  it("errors if it cannot find the external identifier") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) {
        case (bagRootLocation, storageSpace) =>
          withBagAuditor { bagAuditor =>
            val bagInfoLocation = bagRootLocation.join("bag-info.txt")
            s3Client.deleteObject(
              bagInfoLocation.namespace,
              bagInfoLocation.key
            )

            val maybeAudit = bagAuditor.getAuditSummary(
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = CreateIngestType,
              root = bagRootLocation,
              storageSpace = storageSpace
            )

            val result = maybeAudit.success.get

            result shouldBe a[IngestFailed[_]]

            val ingestFailed = result.asInstanceOf[IngestFailed[_]]
            ingestFailed.summary shouldBe a[AuditFailureSummary]
            ingestFailed.maybeUserFacingMessage shouldBe Some(
              "An external identifier was not found in the bag info")
          }
      }
    }
  }

  it("fails if you ask for ingestType 'update' on a new bag") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) {
        case (bagRootLocation, storageSpace) =>
          withBagAuditor { bagAuditor =>
            val maybeAudit = bagAuditor.getAuditSummary(
              ingestId = createIngestID,
              ingestDate = Instant.now,
              ingestType = UpdateIngestType,
              root = bagRootLocation,
              storageSpace = storageSpace
            )

            val result = maybeAudit.success.get

            result shouldBe a[IngestFailed[_]]

            val ingestFailed = result.asInstanceOf[IngestFailed[_]]
            ingestFailed.summary shouldBe a[AuditFailureSummary]
            ingestFailed.maybeUserFacingMessage shouldBe Some(
              "Cannot update existing bag: a bag with the supplied external identifier does not exist in this space")
          }
      }
    }
  }

  it("fails if you ask for ingestType 'create' on an existing bag") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) {
        case (bagRootLocation, storageSpace) =>
          withBagAuditor { bagAuditor =>
            bagAuditor.getAuditSummary(
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(1),
              ingestType = CreateIngestType,
              root = bagRootLocation,
              storageSpace = storageSpace
            )

            val maybeAudit = bagAuditor.getAuditSummary(
              ingestId = createIngestID,
              ingestDate = Instant.ofEpochSecond(2),
              ingestType = CreateIngestType,
              root = bagRootLocation,
              storageSpace = storageSpace
            )

            val result = maybeAudit.success.get

            result shouldBe a[IngestFailed[_]]

            val ingestFailed = result.asInstanceOf[IngestFailed[_]]
            ingestFailed.summary shouldBe a[AuditFailureSummary]
            ingestFailed.maybeUserFacingMessage shouldBe Some(
              "Cannot create new bag: a bag with the supplied external identifier already exists in this space")
          }
      }
    }
  }
}
