package uk.ac.wellcome.platform.storage.replica_aggregator

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.archive.common.fixtures.S3BagBuilder
import uk.ac.wellcome.platform.archive.common.generators.PayloadGenerators
import uk.ac.wellcome.platform.storage.replica_aggregator.fixtures.ReplicaAggregatorFixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures

class ReplicaAggregatorFeatureTest
  extends FunSpec
    with Matchers
    with ReplicaAggregatorFixtures
    with S3Fixtures
    with PayloadGenerators {

  it("fails") {
    withLocalSqsQueueAndDlq { queuePair =>
      val ingests = new MemoryMessageSender()
      val outgoing = new MemoryMessageSender()

      withLocalS3Bucket { bucket =>
        val (bagRootLocation, bagInfo) = S3BagBuilder.createS3BagWith(bucket)

        withReplicaAggregatorWorker(queuePair.queue, ingests, outgoing, stepName = "aggregating replicas") {
          _ =>

            val payload = createEnrichedBagInformationPayloadWith(
              context = createPipelineContextWith(
                externalIdentifier = bagInfo.externalIdentifier
              ),
              bagRootLocation = bagRootLocation
            )

            sendNotificationToSQS(queuePair.queue, payload)

            true shouldBe false
        }
      }
    }
  }
}
