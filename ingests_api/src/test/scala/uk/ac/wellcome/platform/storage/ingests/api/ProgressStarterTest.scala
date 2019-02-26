package uk.ac.wellcome.platform.storage.ingests.api

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.fixtures.SNS
import uk.ac.wellcome.messaging.fixtures.SNS.Topic
import uk.ac.wellcome.platform.archive.common.generators.ProgressGenerators
import uk.ac.wellcome.platform.archive.common.models.{IngestBagRequest, StorageSpace, UnpackRequest}
import uk.ac.wellcome.platform.archive.common.progress.fixtures.ProgressTrackerFixture
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.dynamo._

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
          withProgressStarter(table, archivistTopic, unpackerTopic) { progressStarter =>
            whenReady(
              progressStarter.initialise(progress)) { p =>

              p shouldBe progress

              assertTableOnlyHasItem(progress, table)

              // Archivist
              val archivistRequests =
                listMessagesReceivedFromSNS(
                  archivistTopic
                ).map(messageInfo =>
                  fromJson[IngestBagRequest](
                    messageInfo.message).get
                )

              archivistRequests shouldBe List(
                IngestBagRequest(
                  p.id,
                  storageSpace =
                    StorageSpace(p.space.underlying),
                  archiveCompleteCallbackUrl =
                    p.callback.map(_.uri),
                  zippedBagLocation = ObjectLocation(
                    progress.sourceLocation.location.namespace,
                    progress.sourceLocation.location.key
                  )
                )
              )

              // Unpacker
              val unpackerRequests =
                listMessagesReceivedFromSNS(
                  unpackerTopic
                ).map(messageInfo =>
                  fromJson[UnpackRequest](
                    messageInfo.message).get
                )

              unpackerRequests shouldBe List(
                UnpackRequest(
                  requestId = progress.id,
                  sourceLocation =
                    progress.sourceLocation.location,
                  storageSpace =
                    StorageSpace(progress.space.underlying)
                )
              )
            }
          }
        }
      }
    }
  }

  it("returns a failed future if saving to DynamoDB fails") {
    withLocalSnsTopic { archivistTopic =>
      withLocalSnsTopic { unpackerTopic =>

        val fakeTable = Table("does-not-exist", index = "does-not-exist")

        withProgressStarter(
          fakeTable, archivistTopic, unpackerTopic
        ) { progressStarter =>
          val future = progressStarter.initialise(progress)

          whenReady(future.failed) { _ =>
            assertSnsReceivesNothing(archivistTopic)
            assertSnsReceivesNothing(unpackerTopic)
          }
        }
      }
    }
  }

  it("returns a failed future if publishing to SNS fails") {
    withProgressTrackerTable { table =>
      val fakeArchivistTopic = Topic("does-not-exist")
      val fakeUnpackerTopic = Topic("does-not-exist")

      withProgressStarter(
        table, fakeArchivistTopic, fakeUnpackerTopic
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
    archivistTopic: Topic,
    unpackerTopic: Topic
  )(
    testWith: TestWith[ProgressStarter, R]
  ): R =
    withSNSWriter(archivistTopic) { archivistSnsWriter =>
      withSNSWriter(unpackerTopic) { unpackerSnsWriter =>
        withProgressTracker(table) { progressTracker =>

          val progressStarter = new ProgressStarter(
            progressTracker = progressTracker,
            archivistSnsWriter = archivistSnsWriter,
            unpackerSnsWriter = unpackerSnsWriter
          )

          testWith(progressStarter)
        }
      }
    }

}
