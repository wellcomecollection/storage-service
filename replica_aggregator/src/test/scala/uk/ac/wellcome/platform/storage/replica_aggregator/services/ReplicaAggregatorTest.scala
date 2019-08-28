package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.generators.StorageLocationGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryStorageLocation,
  ReplicaResult,
  StorageLocation
}
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}
import uk.ac.wellcome.storage.{StoreWriteError, UpdateWriteError, Version}

class ReplicaAggregatorTest
    extends FunSpec
    with Matchers
    with EitherValues
    with TryValues
    with StorageLocationGenerators
    with StorageRandomThings {

  def createReplicaResultWith(
    storageLocation: StorageLocation = PrimaryStorageLocation(
      provider = InfrequentAccessStorageProvider,
      prefix = createObjectLocationPrefix
    )
  ): ReplicaResult =
    ReplicaResult(
      storageLocation = storageLocation,
      timestamp = Instant.now
    )

  def createReplicaResult: ReplicaResult =
    createReplicaResultWith()

  def withAggregator[R](
    versionedStore: MemoryVersionedStore[
      ReplicaPath,
      AggregatorInternalRecord
    ] = MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
      initialEntries = Map.empty
    )
  )(testWith: TestWith[ReplicaAggregator, R]): R =
    testWith(
      new ReplicaAggregator(versionedStore)
    )

  describe("handling a primary replica") {
    val primaryLocation = createPrimaryLocation

    val replicaResult = createReplicaResultWith(
      storageLocation = primaryLocation
    )

    val versionedStore =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      )

    val result =
      withAggregator(versionedStore) {
        _.aggregate(replicaResult)
      }

    val expectedRecord =
      AggregatorInternalRecord(
        location = Some(primaryLocation),
        replicas = List.empty
      )

    it("returns the correct record") {
      result.right.value shouldBe expectedRecord
    }

    it("stores the replica in the underlying store") {
      val path = ReplicaPath(replicaResult.storageLocation.prefix.path)

      versionedStore
        .getLatest(path)
        .right
        .value
        .identifiedT shouldBe expectedRecord
    }
  }

  describe("handling a secondary replica") {
    val secondaryLocation = createSecondaryLocation

    val replicaResult = createReplicaResultWith(
      storageLocation = secondaryLocation
    )

    val versionedStore =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      )

    val result =
      withAggregator(versionedStore) {
        _.aggregate(replicaResult)
      }

    val expectedRecord =
      AggregatorInternalRecord(
        location = None,
        replicas = List(secondaryLocation)
      )

    it("returns the correct record") {
      result.right.value shouldBe expectedRecord
    }

    it("stores the replica in the underlying store") {
      val path = ReplicaPath(replicaResult.storageLocation.prefix.path)

      versionedStore
        .getLatest(path)
        .right
        .value
        .identifiedT shouldBe expectedRecord
    }
  }

  describe("handling multiple updates to the same replica") {
    val prefix = createObjectLocationPrefix

    val location1 = createSecondaryLocationWith(
      prefix = prefix.copy(namespace = randomAlphanumeric)
    )
    val location2 = createPrimaryLocationWith(
      prefix = prefix.copy(namespace = randomAlphanumeric)
    )
    val location3 = createSecondaryLocationWith(
      prefix = prefix.copy(namespace = randomAlphanumeric)
    )

    val locations = Seq(location1, location2, location3)

    val versionedStore =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      )

    val results =
      withAggregator(versionedStore) { aggregator =>
        locations
          .map { storageLocation =>
            createReplicaResultWith(storageLocation = storageLocation)
          }
          .map { replicaResult =>
            aggregator.aggregate(replicaResult)
          }
          .map { _.right.value }
      }

    it("returns the correct records") {
      results shouldBe Seq(
        AggregatorInternalRecord(
          location = None,
          replicas = List(location1)
        ),
        AggregatorInternalRecord(
          location = Some(location2),
          replicas = List(location1)
        ),
        AggregatorInternalRecord(
          location = Some(location2),
          replicas = List(location1, location3)
        )
      )
    }

    it("stores the replica in the underlying store") {
      val path = ReplicaPath(prefix.path)

      val expectedRecord =
        AggregatorInternalRecord(
          location = Some(location2),
          replicas = List(location1, location3)
        )

      versionedStore
        .getLatest(path)
        .right
        .value
        .identifiedT shouldBe expectedRecord
    }
  }

  it("stores different replicas under different paths") {
    val primaryLocation1 = createPrimaryLocation
    val primaryLocation2 = createPrimaryLocation

    val versionedStore =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      )

    withAggregator(versionedStore) { aggregator =>
      Seq(primaryLocation1, primaryLocation2)
        .map { storageLocation =>
          createReplicaResultWith(storageLocation = storageLocation)
        }
        .foreach { replicaResult =>
          aggregator.aggregate(replicaResult)
        }
    }

    versionedStore.store
      .asInstanceOf[MemoryStore[
        Version[ReplicaPath, Int],
        AggregatorInternalRecord
      ]]
      .entries should have size 2
  }

  it("handles an error from the underlying versioned store") {
    val err = StoreWriteError(new Throwable("BOOM!"))

    val brokenStore =
      new MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        store =
          new MemoryStore[Version[ReplicaPath, Int], AggregatorInternalRecord](
            initialEntries = Map.empty
          ) with MemoryMaxima[ReplicaPath, AggregatorInternalRecord]
      ) {
        override def upsert(id: ReplicaPath)(t: AggregatorInternalRecord)(
          f: UpdateFunction
        ): UpdateEither = Left(UpdateWriteError(err))
      }

    val result =
      withAggregator(brokenStore)(_.aggregate(createReplicaResult))

    result.left.value shouldBe UpdateWriteError(err)
  }

  it("accepts adding the same primary location to a record twice") {
    val primaryLocation = createPrimaryLocation

    val replicaResult = createReplicaResultWith(
      storageLocation = primaryLocation
    )

    val expectedRecord =
      AggregatorInternalRecord(
        location = Some(primaryLocation),
        replicas = List.empty
      )

    withAggregator() { aggregator =>
      (1 to 5).map { _ =>
        aggregator
          .aggregate(replicaResult)
          .right
          .value shouldBe expectedRecord
      }
    }
  }

  it("fails if you add different primary locations for the same replica path") {
    val prefix = createObjectLocationPrefix

    val primaryLocation1 = createPrimaryLocationWith(
      prefix = prefix.copy(namespace = randomAlphanumeric)
    )

    val primaryLocation2 = createPrimaryLocationWith(
      prefix = prefix.copy(namespace = randomAlphanumeric)
    )

    val replicaResult1 = createReplicaResultWith(primaryLocation1)
    val replicaResult2 = createReplicaResultWith(primaryLocation2)

    withAggregator() { aggregator =>
      val result = aggregator.aggregate(replicaResult1).right.value

      result shouldBe AggregatorInternalRecord(
        location = Some(primaryLocation1),
        replicas = List()
      )

      val err = aggregator.aggregate(replicaResult2).left.get
      err.e.getMessage should startWith(
        "Record already has a different PrimaryStorageLocation"
      )
    }
  }

  it("only stores unique replica results") {
    val replicaResult = createReplicaResult

    val results =
      withAggregator() { aggregator =>
        (1 to 3).map { _ =>
          aggregator.aggregate(replicaResult)
        }
      }

    val uniqResults = results.map { _.right.value }.toSet

    uniqResults should have size 1

    uniqResults.head shouldBe AggregatorInternalRecord(
      replicaResult.storageLocation
    )
  }
}
