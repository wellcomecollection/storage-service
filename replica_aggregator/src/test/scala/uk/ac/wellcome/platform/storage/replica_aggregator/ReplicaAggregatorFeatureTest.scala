package uk.ac.wellcome.platform.storage.replica_aggregator

import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.EnrichedBagInformationPayload
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestUpdateAssertions
import uk.ac.wellcome.platform.storage.replica_aggregator.fixtures.ReplicaAggregatorFixtures

class ReplicaAggregatorFeatureTest
    extends FunSpec
    with Matchers
    with ReplicaAggregatorFixtures
    with IngestUpdateAssertions
    with PayloadGenerators
    with Eventually
    with IntegrationPatience {

  it("passes through the message ") {
    withLocalSqsQueueAndDlq { queuePair =>
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      val payload = createEnrichedBagInformationPayload

      withReplicaAggregatorWorker(
        queuePair.queue,
        ingests,
        outgoing,
        stepName = "aggregating replicas"
      ) { _ =>
        sendNotificationToSQS(queuePair.queue, payload)

        eventually {
          assertTopicReceivesIngestEvents(
            ingestId = payload.ingestId,
            ingests = ingests,
            expectedDescriptions = Seq(
              "Aggregating replicas succeeded"
            )
          )

          outgoing.getMessages[EnrichedBagInformationPayload] shouldBe Seq(
            payload
          )
        }
      }
    }
  }
}
