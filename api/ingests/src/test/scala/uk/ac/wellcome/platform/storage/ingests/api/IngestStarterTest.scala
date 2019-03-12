package uk.ac.wellcome.platform.storage.ingests.api

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.generators.IngestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.archive.common.ingests.models.UnpackBagRequest
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.concurrent.ExecutionContext.Implicits.global

class IngestStarterTest
    extends FunSpec
    with IngestTrackerFixture
    with SNS
    with IngestGenerators
    with ScalaFutures
    with Matchers {

  val ingest = createIngest

  it("saves an Ingest and sends a notification") {
    withLocalSnsTopic { unpackerTopic =>
      withIngestTrackerTable { table =>
        withIngestStarter(table, unpackerTopic) { ingestStarter =>
          whenReady(ingestStarter.initialise(ingest)) { p =>
            p shouldBe ingest

            assertTableOnlyHasItem(ingest, table)

            eventually {
              // Unpacker
              val unpackerRequests =
                listMessagesReceivedFromSNS(
                  unpackerTopic
                ).map(messageInfo =>
                  fromJson[UnpackBagRequest](messageInfo.message).get)

              unpackerRequests shouldBe List(
                UnpackBagRequest(
                  requestId = ingest.id,
                  sourceLocation = ingest.sourceLocation.location,
                  storageSpace = StorageSpace(ingest.space.underlying)
                )
              )
            }
          }

        }
      }
    }
  }

  it("returns a failed future if saving to DynamoDB fails") {
    withLocalSnsTopic { unpackerTopic =>
      val fakeTable = Table("does-not-exist", index = "does-not-exist")

      withIngestStarter(
        fakeTable,
        unpackerTopic
      ) { ingestStarter =>
        val future = ingestStarter.initialise(ingest)

        whenReady(future.failed) { _ =>
          assertSnsReceivesNothing(unpackerTopic)
        }
      }
    }

  }

  it("returns a failed future if publishing to SNS fails") {
    withIngestTrackerTable { table =>
      val fakeUnpackerTopic = Topic("does-not-exist")

      withIngestStarter(
        table,
        fakeUnpackerTopic
      ) { ingestStarter =>
        val future = ingestStarter.initialise(ingest)

        whenReady(future.failed) { _ =>
          assertTableOnlyHasItem(ingest, table)
        }
      }
    }
  }

  private def withIngestStarter[R](
    table: Table,
    unpackerTopic: Topic
  )(
    testWith: TestWith[IngestStarter, R]
  ): R =
    withSNSWriter(unpackerTopic) { unpackerSnsWriter =>
      withIngestTracker(table) { ingestTracker =>
        val ingestStarter = new IngestStarter(
          ingestTracker = ingestTracker,
          unpackerSnsWriter = unpackerSnsWriter
        )

        testWith(ingestStarter)
      }
    }

}
