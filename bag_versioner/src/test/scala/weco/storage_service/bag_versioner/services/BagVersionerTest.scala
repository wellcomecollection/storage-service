package weco.storage_service.bag_versioner.services

import java.time.Instant

import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import weco.storage_service.ingests.models.{CreateIngestType, UpdateIngestType}
import weco.storage_service.storage.models.{IngestFailed, IngestShouldRetry}
import weco.storage_service.bag_versioner.fixtures.BagVersionerFixtures
import weco.storage_service.bag_versioner.models.{
  BagVersionerFailureSummary,
  BagVersionerSuccessSummary
}

class BagVersionerTest
    extends AnyFunSpec
    with Matchers
    with TryValues
    with BagVersionerFixtures
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators {

  it("assigns v1 for a new bag") {
    val externalIdentifier = createExternalIdentifier
    val storageSpace = createStorageSpace

    withBagVersioner { bagVersioner =>
      val maybeVersion = bagVersioner.getSummary(
        ingestId = createIngestID,
        ingestDate = Instant.now,
        ingestType = CreateIngestType,
        externalIdentifier = externalIdentifier,
        storageSpace = storageSpace
      )

      val result = maybeVersion.success.get
      result.maybeUserFacingMessage shouldBe Some("Assigned bag version v1")

      val summary = result.summary
        .asInstanceOf[BagVersionerSuccessSummary]

      summary.version shouldBe BagVersion(1)
    }
  }

  it("is retryable if it encounters a lock failure") {
    val externalIdentifier = createExternalIdentifier
    val storageSpace = createStorageSpace
    val lockDao = createBrokenLockDao

    withBagVersioner(lockDao) { bagVersioner =>
      val maybeVersion = bagVersioner.getSummary(
        ingestId = createIngestID,
        ingestDate = Instant.now,
        ingestType = CreateIngestType,
        externalIdentifier = externalIdentifier,
        storageSpace = storageSpace
      )

      val result = maybeVersion.success.get
      result shouldBe a[IngestShouldRetry[_]]
    }
  }

  it("fails if you ask for ingestType 'update' on a new bag") {
    val externalIdentifier = createExternalIdentifier
    val storageSpace = createStorageSpace

    withBagVersioner { bagVersioner =>
      val maybeVersion = bagVersioner.getSummary(
        ingestId = createIngestID,
        ingestDate = Instant.now,
        ingestType = UpdateIngestType,
        externalIdentifier = externalIdentifier,
        storageSpace = storageSpace
      )

      val result = maybeVersion.success.get
      result shouldBe a[IngestFailed[_]]

      result.summary shouldBe a[BagVersionerFailureSummary]
      result.maybeUserFacingMessage shouldBe Some(
        "Cannot update existing bag: a bag with the supplied external identifier does not exist in this space"
      )
    }
  }

  it("fails if you ask for ingestType 'create' on an existing bag") {
    val externalIdentifier = createExternalIdentifier
    val storageSpace = createStorageSpace

    withBagVersioner { bagVersioner =>
      bagVersioner.getSummary(
        ingestId = createIngestID,
        ingestDate = Instant.ofEpochSecond(1),
        ingestType = CreateIngestType,
        externalIdentifier = externalIdentifier,
        storageSpace = storageSpace
      )

      val maybeVersion = bagVersioner.getSummary(
        ingestId = createIngestID,
        ingestDate = Instant.ofEpochSecond(2),
        ingestType = CreateIngestType,
        externalIdentifier = externalIdentifier,
        storageSpace = storageSpace
      )

      val result = maybeVersion.success.get
      result shouldBe a[IngestFailed[_]]

      result.summary shouldBe a[BagVersionerFailureSummary]
      result.maybeUserFacingMessage shouldBe Some(
        "Cannot create new bag: a bag with the supplied external identifier already exists in this space"
      )
    }
  }
}
