package uk.ac.wellcome.platform.storage.replica_aggregator

import java.time.Instant

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{EitherValues, FunSpec, Inside, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.storage.replica_aggregator.fixtures.ReplicaAggregatorFixtures
import uk.ac.wellcome.platform.storage.replica_aggregator.models.{PrimaryStorageLocation, ReplicaPath, ReplicaResult}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

class ReplicaAggregatorFeatureTest
    extends FunSpec
    with Matchers
    with ReplicaAggregatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with Eventually
    with EitherValues
    with Inside
    with IntegrationPatience {

  it("completes after a single primary replica") {
    withLocalSqsQueue { queue =>
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      val payload = createEnrichedBagInformationPayload
      val versionedStore =
        MemoryVersionedStore[ReplicaPath, List[ReplicaResult]](Map.empty)

      withReplicaAggregatorWorker(
        queue = queue,
        versionedStore = versionedStore,
        ingests = ingests,
        outgoing = outgoing,
        stepName = "aggregating replicas"
      ) { _ =>
        sendNotificationToSQS(queue, payload)

        eventually {
          assertTopicReceivesIngestEvents(
            ingestId = payload.ingestId,
            ingests = ingests,
            expectedDescriptions = Seq(
              "Aggregating replicas succeeded"
            )
          )

          val expectedReplicaPath =
            ReplicaPath(payload.bagRootLocation.path)

          val stored =
            versionedStore.get(id = Version(expectedReplicaPath, 0)).right.value

          stored.identifiedT.length shouldBe 1

          val replicaResult = stored.identifiedT.head

          inside(replicaResult) {
            case ReplicaResult(
                ingestId,
                PrimaryStorageLocation(
                  storageProvider,
                  location
                ),
                timestamp
                ) =>
              ingestId shouldBe payload.ingestId
              storageProvider shouldBe InfrequentAccessStorageProvider
              location shouldBe payload.bagRootLocation
              timestamp shouldBe a[Instant]
          }

          outgoing.getMessages[EnrichedBagInformationPayload] shouldBe Seq(
            payload
          )
        }
      }
    }
  }
}
