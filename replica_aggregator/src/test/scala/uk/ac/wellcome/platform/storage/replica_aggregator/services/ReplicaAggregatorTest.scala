package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import org.scalatest.{FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
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

  it("stores a single primary replica") {
    val replicaResult = ReplicaResult(
      ingestId = createIngestID,
      location = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = createObjectLocation
      ),
      timestamp = Instant.now
    )

    withAggregator {
      _.aggregate(replicaResult)
    }

    // Assert versionedStore has the replica!
  }

  it("errors if asked to aggregate a secondary replica") {
    val replicaResult = ReplicaResult(
      ingestId = createIngestID,
      location = SecondaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = createObjectLocation
      ),
      timestamp = Instant.now
    )

    val result =
      withAggregator {
        _.aggregate(replicaResult)
      }

    val throwable = result.failed.get
    throwable.getMessage shouldBe s"Cannot aggregate secondary replica result: $replicaResult"
  }

  // versioned store error => error

  // duplicates ignored
}
