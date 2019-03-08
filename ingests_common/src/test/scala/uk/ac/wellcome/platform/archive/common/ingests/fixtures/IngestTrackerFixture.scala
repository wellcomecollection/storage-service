package uk.ac.wellcome.platform.archive.common.ingests.fixtures

import java.util.UUID

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingest.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker

import scala.concurrent.ExecutionContext.Implicits.global

trait IngestTrackerFixture
    extends IngestTrackerDynamoDb
    with RandomThings
    with IngestGenerators
    with TimeTestFixture {

  import uk.ac.wellcome.storage.dynamo._

  def createTable(table: LocalDynamoDb.Table): Table = Table("table", "index")

  def withIngestTracker[R](table: Table,
                           dynamoDbClient: AmazonDynamoDB = dynamoDbClient)(
    testWith: TestWith[IngestTracker, R]): R = {
    val ingestTracker = new IngestTracker(
      dynamoDbClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )
    testWith(ingestTracker)
  }

  def assertIngestCreated(ingest: Ingest, table: Table): Ingest = {
    val storedIngest =
      getExistingTableItem[Ingest](ingest.id.toString, table)
    storedIngest.sourceLocation shouldBe ingest.sourceLocation

    assertRecent(storedIngest.createdDate, recentSeconds = 45)
    assertRecent(storedIngest.lastModifiedDate, recentSeconds = 45)
    storedIngest
  }

  def assertIngestRecordedRecentEvents(id: UUID,
                                       expectedEventDescriptions: Seq[String],
                                       table: LocalDynamoDb.Table): Unit = {
    val ingest = getExistingTableItem[Ingest](id.toString, table)

    ingest.events.map(_.description) should contain theSameElementsAs expectedEventDescriptions
    ingest.events.foreach(event =>
      assertRecent(event.createdDate, recentSeconds = 45))
  }

}
