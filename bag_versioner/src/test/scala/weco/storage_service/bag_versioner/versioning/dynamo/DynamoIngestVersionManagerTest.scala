package weco.storage_service.bag_versioner.versioning.dynamo

import org.scanamo.generic.auto._
import weco.fixtures.TestWith
import weco.storage_service.bagit.models.ExternalIdentifier
import weco.storage_service.ingests.models.IngestID
import weco.storage_service.storage.models.StorageSpace
import weco.storage_service.bag_versioner.versioning.{
  IngestVersionManager,
  IngestVersionManagerTestCases,
  VersionRecord
}
import weco.storage.{MaximaError, MaximaReadError}
import weco.storage.fixtures.DynamoFixtures.Table

import scala.language.higherKinds
import scala.util.{Failure, Try}

class DynamoIngestVersionManagerTest
    extends IngestVersionManagerTestCases[DynamoIngestVersionManagerDao, Table]
    with IngestVersionManagerTable {
  override def withDao[R](
    testWith: TestWith[DynamoIngestVersionManagerDao, R]
  )(implicit table: Table): R =
    testWith(
      new DynamoIngestVersionManagerDao(
        dynamoClient = dynamoClient,
        dynamoConfig = createDynamoConfigWith(table)
      )
    )

  override def withBrokenLookupExistingVersionDao[R](
    testWith: TestWith[DynamoIngestVersionManagerDao, R]
  )(implicit table: Table): R =
    testWith(
      new DynamoIngestVersionManagerDao(
        dynamoClient = dynamoClient,
        dynamoConfig = createDynamoConfigWith(table)
      ) {
        override def lookupExistingVersion(
          ingestId: IngestID
        ): Try[Option[VersionRecord]] =
          Failure(new Throwable("BOOM!"))
      }
    )

  override def withBrokenLookupLatestVersionForDao[R](
    testWith: TestWith[DynamoIngestVersionManagerDao, R]
  )(implicit table: Table): R =
    testWith(
      new DynamoIngestVersionManagerDao(
        dynamoClient = dynamoClient,
        dynamoConfig = createDynamoConfigWith(table)
      ) {
        override def lookupLatestVersionFor(
                                             externalIdentifier: ExternalIdentifier,
                                             space: StorageSpace
        ): Either[MaximaError, VersionRecord] =
          Left(MaximaReadError(new Throwable("BOOM!")))
      }
    )

  override def withBrokenStoreNewVersionDao[R](
    testWith: TestWith[DynamoIngestVersionManagerDao, R]
  )(implicit table: Table): R =
    testWith(
      new DynamoIngestVersionManagerDao(
        dynamoClient = dynamoClient,
        dynamoConfig = createDynamoConfigWith(table)
      ) {
        override def storeNewVersion(record: VersionRecord): Try[Unit] =
          Failure(new Throwable("BOOM!"))
      }
    )

  override def withManager[R](
    dao: DynamoIngestVersionManagerDao
  )(testWith: TestWith[IngestVersionManager, R])(implicit context: Table): R =
    testWith(
      new DynamoIngestVersionManager(dao)
    )
}
