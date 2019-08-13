package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.storage.replica_aggregator.models.{PrimaryStorageLocation, ReplicaResult, ReplicationAggregationComplete}
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class ReplicaAggregatorTest
  extends FunSpec
    with Matchers
    with TryValues
    with ObjectLocationGenerators
    with StorageRandomThings {

  def withAggregator[R](
    expectedReplicaCount: Int = randomInt(from = 1, to = 10)
  )(testWith: TestWith[ReplicaAggregator, R]): R =
    testWith(
      new ReplicaAggregator(
        expectedReplicaCount = expectedReplicaCount
      )
    )

  it("stores a single replica") {
    withAggregator(expectedReplicaCount = 1) { aggregator =>
      val result = aggregator.aggregate(
        ReplicaResult(
          ingestId = createIngestID,
          location = PrimaryStorageLocation(
            provider = InfrequentAccessStorageProvider,
            location = createObjectLocation
          ),
          timestamp = Instant.now
        )
      )

      result.success.value.summary shouldBe a[ReplicationAggregationComplete]
    }
  }
}
