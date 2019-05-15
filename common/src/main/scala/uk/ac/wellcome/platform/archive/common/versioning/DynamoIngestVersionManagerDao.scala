package uk.ac.wellcome.platform.archive.common.versioning

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.storage.dynamo.DynamoConfig

import scala.util.{Failure, Success, Try}

class DynamoIngestVersionManagerDao(
  dynamoClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
) extends IngestVersionManagerDao {
  override def lookupExistingVersion(ingestID: IngestID): Try[Option[VersionRecord]] = Success(None)

  override def lookupLatestVersionFor(externalIdentifier: ExternalIdentifier): Try[Option[VersionRecord]] = Failure(new Throwable("BOOM!"))

  override def storeNewVersion(record: VersionRecord): Try[Unit] = Failure(new Throwable("BOOM!"))
}
