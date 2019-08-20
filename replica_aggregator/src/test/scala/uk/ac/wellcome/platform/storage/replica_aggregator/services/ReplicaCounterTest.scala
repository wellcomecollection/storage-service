package uk.ac.wellcome.platform.storage.replica_aggregator.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.storage.replica_aggregator.generators.StorageLocationGenerators
import uk.ac.wellcome.platform.storage.replica_aggregator.models.{AggregatorInternalRecord, KnownReplicas}

class ReplicaCounterTest extends FunSpec with Matchers with EitherValues with StorageLocationGenerators {
  it("rejects a record without a primary location") {
    val counter = new ReplicaCounter(expectedReplicaCount = 1)

    val record = AggregatorInternalRecord(
      location = None,
      replicas = List.empty
    )

    counter.countReplicas(record).left.value shouldBe NoPrimaryReplica()
  }

  it("rejects a record without a primary location even if it has enough secondary replicas") {
    val counter = new ReplicaCounter(expectedReplicaCount = 3)

    val record = AggregatorInternalRecord(
      location = None,
      replicas = List(createSecondaryLocation, createSecondaryLocation, createSecondaryLocation)
    )

    counter.countReplicas(record).left.value shouldBe NoPrimaryReplica()
  }

  it("if expectedCount = 1, a single primary replica is enough") {
    val counter = new ReplicaCounter(expectedReplicaCount = 1)

    val location = createPrimaryLocation

    val record = AggregatorInternalRecord(
      location = Some(location),
      replicas = List.empty
    )

    counter.countReplicas(record).right.value shouldBe KnownReplicas(
      location = location,
      replicas = List.empty
    )
  }

  it("if expectedCount = 3, it expects a primary replica and two secondaries") {
    val counter = new ReplicaCounter(expectedReplicaCount = 3)

    val location = createPrimaryLocation
    val replicas = List(createSecondaryLocation, createSecondaryLocation)

    val record = AggregatorInternalRecord(
      location = Some(location),
      replicas = replicas
    )

    counter.countReplicas(record).right.value shouldBe KnownReplicas(
      location = location,
      replicas = replicas
    )
  }

  it("rejects a record without enough secondary replicas") {
    val counter = new ReplicaCounter(expectedReplicaCount = 5)

    val location = createPrimaryLocation
    val replicas = List(createSecondaryLocation, createSecondaryLocation)

    val record = AggregatorInternalRecord(
      location = Some(location),
      replicas = replicas
    )

    counter.countReplicas(record).left.value shouldBe NotEnoughReplicas(
      expected = 5,
      actual = 3
    )
  }

  it("allows a record with extra replicas") {
    val counter = new ReplicaCounter(expectedReplicaCount = 3)

    val location = createPrimaryLocation
    val replicas = (1 to 5).map { _ => createSecondaryLocation }.toList

    val record = AggregatorInternalRecord(
      location = Some(location),
      replicas = replicas
    )

    counter.countReplicas(record).right.value shouldBe KnownReplicas(
      location = location,
      replicas = replicas
    )
  }
}
