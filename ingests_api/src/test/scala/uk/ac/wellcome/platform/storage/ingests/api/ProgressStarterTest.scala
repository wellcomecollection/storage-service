package uk.ac.wellcome.platform.storage.ingests.api

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.generators.ProgressGenerators
import uk.ac.wellcome.platform.archive.common.models.{StorageSpace, UnpackBagRequest}
import uk.ac.wellcome.platform.archive.common.progress.fixtures.ProgressTrackerFixture
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.concurrent.ExecutionContext.Implicits.global

class ProgressStarterTest
    extends FunSpec
    with ProgressTrackerFixture
    with SNS
    with ProgressGenerators
    with ScalaFutures
    with Matchers {

  val progress = createProgress

  it("saves a Progress to DynamoDB and send a notification to SNS") {
    withLocalSnsTopic { archivistTopic =>
      withLocalSnsTopic { unpackerTopic =>
        withProgressTrackerTable { table =>
          withProgressStarter(table, archivistTopic) {
            progressStarter =>
              whenReady(progressStarter.initialise(progress)) { p =>
                p shouldBe progress

                assertTableOnlyHasItem(progress, table)

                // Unpacker
                val unpackerRequests =
                  listMessagesReceivedFromSNS(
                    unpackerTopic
                  ).map(messageInfo =>
                    fromJson[UnpackBagRequest](messageInfo.message).get)

                unpackerRequests shouldBe List(
                  UnpackBagRequest(
                    requestId = progress.id,
                    sourceLocation = progress.sourceLocation.location,
                    storageSpace = StorageSpace(progress.space.underlying)
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

        withProgressStarter(
          fakeTable,
          unpackerTopic
        ) { progressStarter =>
          val future = progressStarter.initialise(progress)

          whenReady(future.failed) { _ =>
            assertSnsReceivesNothing(unpackerTopic)
          }
        }
      }

  }

  it("returns a failed future if publishing to SNS fails") {
    withProgressTrackerTable { table =>
      val fakeUnpackerTopic = Topic("does-not-exist")

      withProgressStarter(
        table,
        fakeUnpackerTopic
      ) { progressStarter =>
        val future = progressStarter.initialise(progress)

        whenReady(future.failed) { _ =>
          assertTableOnlyHasItem(progress, table)
        }
      }
    }
  }

  private def withProgressStarter[R](
    table: Table,
    unpackerTopic: Topic
  )(
    testWith: TestWith[ProgressStarter, R]
  ): R =
      withSNSWriter(unpackerTopic) { unpackerSnsWriter =>
        withProgressTracker(table) { progressTracker =>
          val progressStarter = new ProgressStarter(
            progressTracker = progressTracker,
            unpackerSnsWriter = unpackerSnsWriter
          )

          testWith(progressStarter)
        }
      }


}
