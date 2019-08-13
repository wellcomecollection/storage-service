package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.storage.replica_aggregator.models.{PrimaryStorageLocation, ReplicaResult, ReplicationAggregationComplete, ReplicationSet}
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class ReplicaAggregatorTest
  extends FunSpec
    with Matchers
    with TryValues
    with ObjectLocationGenerators
    with StorageRandomThings {

  def withAggregator[R](testWith: TestWith[ReplicaAggregator, R]): R =
    testWith(
      new ReplicaAggregator()
    )

  it("completes after a single primary replica") {
    val location = createObjectLocation

    val replicaResult = ReplicaResult(
      ingestId = createIngestID,
      location = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = location
      ),
      timestamp = Instant.now
    )

    val result =
      withAggregator {
        _.aggregate(replicaResult)
      }

    val summary = result.success.value

    summary shouldBe a[ReplicationAggregationComplete]
    summary.asInstanceOf[ReplicationAggregationComplete].replicationSet shouldBe
      ReplicationSet(
        path = location.path,
        results = Set(replicaResult)
      )
  }

  // stores a single primary replica

  // errors on secondary

  // versioned store error => error

  // duplicates ignored
}
