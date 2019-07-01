package uk.ac.wellcome.platform.archive.common.versioning.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.{
  IngestVersionManager,
  IngestVersionManagerTestCases,
  VersionRecord
}
import uk.ac.wellcome.storage.{MaximaError, MaximaReadError}

import scala.util.{Failure, Try}

class MemoryIngestVersionManagerTest
    extends IngestVersionManagerTestCases[
      MemoryIngestVersionManagerDao,
      MemoryIngestVersionManagerDao] {
  override def withContext[R](
    testWith: TestWith[MemoryIngestVersionManagerDao, R]): R =
    testWith(new MemoryIngestVersionManagerDao())

  override def withDao[R](testWith: TestWith[MemoryIngestVersionManagerDao, R])(
    implicit context: MemoryIngestVersionManagerDao): R =
    testWith(context)

  override def withBrokenLookupExistingVersionDao[R](
    testWith: TestWith[MemoryIngestVersionManagerDao, R])(
    implicit context: MemoryIngestVersionManagerDao): R =
    testWith(new MemoryIngestVersionManagerDao() {
      override def lookupExistingVersion(
        ingestID: IngestID): Try[Option[VersionRecord]] =
        Failure(new Throwable("BOOM!"))
    })

  override def withBrokenLookupLatestVersionForDao[R](
    testWith: TestWith[MemoryIngestVersionManagerDao, R])(
    implicit context: MemoryIngestVersionManagerDao): R =
    testWith(new MemoryIngestVersionManagerDao() {
      override def lookupLatestVersionFor(
        externalIdentifier: ExternalIdentifier,
        storageSpace: StorageSpace): Either[MaximaError, VersionRecord] =
        Left(MaximaReadError(new Throwable("BOOM!")))
    })

  override def withBrokenStoreNewVersionDao[R](
    testWith: TestWith[MemoryIngestVersionManagerDao, R])(
    implicit context: MemoryIngestVersionManagerDao): R =
    testWith(new MemoryIngestVersionManagerDao() {
      override def storeNewVersion(record: VersionRecord): Try[Unit] =
        Failure(new Throwable("BOOM!"))
    })

  override def withManager[R](dao: MemoryIngestVersionManagerDao)(
    testWith: TestWith[IngestVersionManager, R])(
    implicit context: MemoryIngestVersionManagerDao): R =
    testWith(
      new MemoryIngestVersionManager(dao)
    )
}
