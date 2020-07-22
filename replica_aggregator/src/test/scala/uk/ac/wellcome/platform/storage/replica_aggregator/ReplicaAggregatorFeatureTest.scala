package uk.ac.wellcome.platform.storage.replica_aggregator

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.KnownReplicasPayload
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models.AmazonS3StorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{
  KnownReplicas,
  PrimaryS3ReplicaLocation,
  PrimaryStorageLocation
}
import uk.ac.wellcome.platform.storage.replica_aggregator.fixtures.ReplicaAggregatorFixtures
import uk.ac.wellcome.platform.storage.replica_aggregator.models.{
  AggregatorInternalRecord,
  ReplicaPath
}
import uk.ac.wellcome.storage.{S3ObjectLocationPrefix, Version}
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore

class ReplicaAggregatorFeatureTest
    extends AnyFunSpec
    with Matchers
    with ReplicaAggregatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with Eventually
    with EitherValues
    with IntegrationPatience {

  it("completes after a single primary replica") {
    withLocalSqsQueue() { queue =>
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      val payload = createReplicaCompletePayloadWith(
        provider = AmazonS3StorageProvider
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
            ingestId = payload.ingestId,
            ingests = ingests,
            expectedDescriptions = Seq(
              "Aggregating replicas succeeded - all replicas complete"
            )
          )

          val expectedReplicaPath =
            ReplicaPath(payload.bagRoot.path)

          val stored =
            versionedStore.get(id = Version(expectedReplicaPath, 0)).right.value

          val primaryLocation = payload.dstLocation.asInstanceOf[PrimaryStorageLocation]

          val primaryReplicaLocation = PrimaryS3ReplicaLocation(
            prefix = S3ObjectLocationPrefix(payload.bagRoot)
          )

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
