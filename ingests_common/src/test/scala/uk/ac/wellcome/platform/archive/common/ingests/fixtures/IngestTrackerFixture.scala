package uk.ac.wellcome.platform.archive.common.ingests.fixtures

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scanamo.DynamoFormat
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingest.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.{Callback, Ingest, IngestID}
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID._
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestType._
import uk.ac.wellcome.platform.archive.common.ingests.models.Namespace._
import uk.ac.wellcome.platform.archive.common.ingests.monitor.IngestTracker
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

trait IngestTrackerFixture
    extends IngestTrackerDynamoDb
    with IngestGenerators
    with TimeTestFixture {

  def createTable(table: Table): Table = Table("table", "index")

  def withIngestTracker[R](table: Table,
                           dynamoDbClient: AmazonDynamoDB = dynamoClient)(
    testWith: TestWith[IngestTracker, R]): R = {
    val ingestTracker = new IngestTracker(
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )
    testWith(ingestTracker)
  }

  def getStoredIngest(ingest: Ingest, table: Table): Ingest =
    getExistingTableItem[Ingest](ingest.id.toString, table)

  def assertIngestCreated(ingest: Ingest, table: Table): Ingest = {
    val storedIngest = getStoredIngest(ingest, table)
    storedIngest.sourceLocation shouldBe ingest.sourceLocation

    assertRecent(storedIngest.createdDate, recentSeconds = 45)
    storedIngest.lastModifiedDate.map { lastModifiedDate =>
      assertRecent(lastModifiedDate, recentSeconds = 45)
    }
    storedIngest
  }

  def assertIngestRecordedRecentEvents(id: IngestID,
                                       expectedEventDescriptions: Seq[String],
                                       table: Table): Unit = {
    val ingest = getExistingTableItem[Ingest](id.toString, table)

    ingest.events.map(_.description) should contain theSameElementsAs expectedEventDescriptions
    ingest.events.foreach(event =>
      assertRecent(event.createdDate, recentSeconds = 45))
  }
}
