//package uk.ac.wellcome.platform.archive.bagunpacker
//
//import java.nio.file.Paths
//
//import org.scalatest.concurrent.{Eventually, IntegrationPatience}
//import org.scalatest.{FunSpec, Matchers}
//import uk.ac.wellcome.json.JsonUtil._
//import uk.ac.wellcome.platform.archive.bagunpacker.fixtures.{BagUnpackerFixtures, CompressFixture}
//import uk.ac.wellcome.platform.archive.common.UnpackedBagLocationPayload
//import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
//import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
//import uk.ac.wellcome.platform.archive.common.ingests.models.{Ingest, IngestStatusUpdate}
//import uk.ac.wellcome.storage.ObjectLocationPrefix
//
//class UnpackerFeatureTest
//    extends FunSpec
//    with Matchers
//    with Eventually
//    with BagUnpackerFixtures
//    with IntegrationPatience
//    with CompressFixture
//    with IngestUpdateAssertions
//    with PayloadGenerators {
//
//  it("receives and processes a notification") {
//    val (archiveFile, _, _) = createTgzArchiveWithRandomFiles()
//    withBagUnpackerApp(stepName = "unpacker") {
//      case (_, srcBucket, queue, ingests, outgoing) =>
//        withArchive(srcBucket, archiveFile) { archiveLocation =>
//          val sourceLocationPayload =
//            createSourceLocationPayloadWith(archiveLocation)
//          sendNotificationToSQS(queue, sourceLocationPayload)
//
//          eventually {
//            val expectedPayload = UnpackedBagLocationPayload(
//              context = sourceLocationPayload.context,
//              unpackedBagLocation = ObjectLocationPrefix(
//                namespace = srcBucket.name,
//                path = Paths
//                  .get(
//                    sourceLocationPayload.storageSpace.toString,
//                    sourceLocationPayload.ingestId.toString
//                  )
//                  .toString
//              )
//            )
//
//            outgoing.getMessages[UnpackedBagLocationPayload] shouldBe Seq(
//              expectedPayload)
//
//            assertTopicReceivesIngestEvents(
//              sourceLocationPayload.ingestId,
//              ingests,
//              expectedDescriptions = Seq(
//                "Unpacker started",
//                "Unpacker succeeded"
//              )
//            )
//          }
//        }
//    }
//  }
//
//  it("sends a failed Ingest update if it cannot read the bag") {
//    withBagUnpackerApp(stepName = "unpacker") {
//      case (_, _, queue, ingests, outgoing) =>
//        val payload = createSourceLocation
//        sendNotificationToSQS(queue, payload)
//
//        eventually {
//          outgoing.messages shouldBe empty
//
//          assertTopicReceivesIngestUpdates(payload.ingestId, ingests) {
//            ingestUpdates =>
//              ingestUpdates.size shouldBe 2
//
//              val ingestStart = ingestUpdates.head
//              ingestStart.events.head.description shouldBe "Unpacker started"
//
//              val ingestFailed =
//                ingestUpdates.tail.head.asInstanceOf[IngestStatusUpdate]
//              ingestFailed.status shouldBe Ingest.Failed
//              ingestFailed.events.head.description shouldBe
//                s"Unpacker failed - ${payload.sourceLocation} could not be downloaded"
//          }
//        }
//    }
//  }
//}
