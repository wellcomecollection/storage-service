package uk.ac.wellcome.platform.archive.common.ingests.fixtures

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import org.scalatest.Assertion
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.IngestID
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingest.fixtures.TimeTestFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest
import uk.ac.wellcome.platform.archive.common.ingests.monitor.{
  DynamoIngestTracker,
  MemoryIngestTracker
}
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

trait IngestTrackerFixture
    extends IngestTrackerDynamoDb
    with IngestGenerators
    with TimeTestFixture {

  def createTable(table: LocalDynamoDb.Table): Table = Table("table", "index")

  def withIngestTracker[R](table: Table,
                           dynamoDbClient: AmazonDynamoDB = dynamoDbClient)(
    testWith: TestWith[DynamoIngestTracker, R]): R = {
    val ingestTracker = new DynamoIngestTracker(
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )
    testWith(ingestTracker)
  }

  def getStoredIngest(ingest: Ingest, table: Table): Ingest =
    getExistingTableItem[Ingest](ingest.id.toString, table)

  def assertIngestCreated(tracker: MemoryIngestTracker)(
    ingest: Ingest): Assertion = {
    val storedIngest = tracker.ingests(ingest.id)

    storedIngest.sourceLocation shouldBe ingest.sourceLocation
    assertRecent(storedIngest.createdDate, recentSeconds = 45)
    assertRecent(storedIngest.lastModifiedDate, recentSeconds = 45)
  }

  def assertIngestCreated(ingest: Ingest, table: Table): Ingest = {
    val storedIngest = getStoredIngest(ingest, table)
    storedIngest.sourceLocation shouldBe ingest.sourceLocation

    assertRecent(storedIngest.createdDate, recentSeconds = 45)
    assertRecent(storedIngest.lastModifiedDate, recentSeconds = 45)
    storedIngest
  }

  def assertIngestRecordedRecentEvents(id: IngestID,
                                       expectedEventDescriptions: Seq[String],
                                       table: LocalDynamoDb.Table): Unit = {
    val ingest = getExistingTableItem[Ingest](id.toString, table)

    ingest.events.map(_.description) should contain theSameElementsAs expectedEventDescriptions
    ingest.events.foreach(event =>
      assertRecent(event.createdDate, recentSeconds = 45))
  }

  def assertIngestRecordedRecentEvents(tracker: MemoryIngestTracker)(
    id: IngestID,
    expectedEventDescriptions: Seq[String]): Unit = {
    val ingest = tracker.ingests(id)

    ingest.events.map(_.description) should contain theSameElementsAs expectedEventDescriptions
    ingest.events.foreach(event =>
      assertRecent(event.createdDate, recentSeconds = 45))
  }
}
