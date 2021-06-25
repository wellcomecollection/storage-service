package weco.storage_service.replica_aggregator

import org.scalatest.EitherValues
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.json.JsonUtil._
import weco.messaging.memory.MemoryMessageSender
import weco.storage_service.KnownReplicasPayload
import weco.storage_service.generators.{
  PayloadGenerators,
  ReplicaLocationGenerators
}
import weco.storage_service.ingests.fixtures.IngestUpdateAssertions
import weco.storage_service.storage.models.{
  KnownReplicas,
  PrimaryS3ReplicaLocation
}
import weco.storage_service.replica_aggregator.fixtures.ReplicaAggregatorFixtures
import weco.storage_service.replica_aggregator.models.{
  AggregatorInternalRecord,
  ReplicaPath
}
import weco.storage.Version
import weco.storage.store.memory.MemoryVersionedStore

class ReplicaAggregatorFeatureTest
    extends AnyFunSpec
    with Matchers
    with ReplicaAggregatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with Eventually
    with EitherValues
    with IntegrationPatience
    with ReplicaLocationGenerators {

  it("completes after a single primary replica") {
    withLocalSqsQueue() { queue =>
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      val primaryReplicaLocation = PrimaryS3ReplicaLocation(
        prefix = createS3ObjectLocationPrefix
      )

      val payload = createReplicaCompletePayloadWith(
        dstLocation = primaryReplicaLocation
      )
      val versionedStore =
        MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](Map.empty)

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
            ingests = ingests,
            expectedDescriptions = Seq(
              "Aggregating replicas succeeded - all replicas complete"
            )
          )

          val expectedReplicaPath = ReplicaPath(payload.dstLocation.prefix)

          val stored =
            versionedStore.get(id = Version(expectedReplicaPath, 0)).value

          val primaryLocation =
            payload.dstLocation.asInstanceOf[PrimaryS3ReplicaLocation]

          stored.identifiedT.location shouldBe Some(primaryReplicaLocation)

          stored.identifiedT.replicas shouldBe empty

          val expectedPayload = KnownReplicasPayload(
            context = payload.context,
            version = payload.version,
            knownReplicas = KnownReplicas(
              location = primaryLocation,
              replicas = List.empty
            )
          )

          outgoing.getMessages[KnownReplicasPayload] shouldBe Seq(
            expectedPayload
          )
        }
      }
    }
  }
}
