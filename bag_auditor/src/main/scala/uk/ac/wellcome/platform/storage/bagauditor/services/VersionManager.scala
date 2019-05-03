package uk.ac.wellcome.platform.storage.bagauditor.services
import java.time.Instant

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.{Scanamo, Table}
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.storage.dynamo._

import scala.concurrent.Future

case class BagVersion(
  ingestId: IngestID,
  ingestDate: Instant,
  externalIdentifier: ExternalIdentifier,
  version: Int
)

class VersionManager(
  dynamoClient: AmazonDynamoDB,
  dynamoConfig: DynamoConfig
) {

  val table: Table[BagVersion] = Table[BagVersion](dynamoConfig.table)

  def assignVersion(
    ingestId: IngestID,
    ingestDate: Instant,
    externalIdentifier: ExternalIdentifier
  ): Future[Int] = {
    val bagVersion = BagVersion(
      ingestId = ingestId,
      ingestDate = ingestDate,
      externalIdentifier = externalIdentifier,
      version = 1
    )

    Scanamo.exec(dynamoClient)(table.put(bagVersion))

    Future.successful(1)
  }
}
