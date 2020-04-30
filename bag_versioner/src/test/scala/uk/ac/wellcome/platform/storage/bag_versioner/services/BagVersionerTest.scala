package uk.ac.wellcome.platform.storage.bag_versioner.services

import java.time.Instant

import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  CreateIngestType,
  UpdateIngestType
}
import uk.ac.wellcome.platform.archive.common.storage.models.IngestFailed
import uk.ac.wellcome.platform.storage.bag_versioner.fixtures.BagVersionerFixtures
import uk.ac.wellcome.platform.storage.bag_versioner.models.{
  BagVersionerFailureSummary,
  BagVersionerSuccessSummary
}
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class BagVersionerTest
    extends AnyFunSpec
    with Matchers
    with TryValues
    with BagVersionerFixtures
    with ObjectLocationGenerators
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
