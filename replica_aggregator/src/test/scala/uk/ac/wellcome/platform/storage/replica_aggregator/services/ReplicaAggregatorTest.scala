package uk.ac.wellcome.platform.storage.replica_aggregator.services

import org.scalatest.{EitherValues, TryValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage.generators.ReplicaLocationGenerators
import weco.storage_service.storage.models._
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import weco.storage.maxima.memory.MemoryMaxima
import weco.storage.store.memory.{MemoryStore, MemoryVersionedStore}
import uk.ac.wellcome.storage.{StoreWriteError, UpdateWriteError, Version}

class ReplicaAggregatorTest
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with TryValues
    with ReplicaLocationGenerators {

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

    val versionedStore =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      )

    val result =
      withAggregator(versionedStore) {
        _.aggregate(primaryLocation)
      }

    val expectedRecord =
      AggregatorInternalRecord(
        location = Some(primaryLocation),
        replicas = List.empty
      )

    it("returns the correct record") {
      result.value shouldBe expectedRecord
    }

    it("stores the replica in the underlying store") {
      val path = ReplicaPath(primaryLocation.prefix)

      versionedStore
        .getLatest(path)
        .value
        .identifiedT shouldBe expectedRecord
    }
  }

  describe("handling a secondary replica") {
    val secondaryLocation = createSecondaryLocation

    val versionedStore =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      )

    val result =
      withAggregator(versionedStore) {
        _.aggregate(secondaryLocation)
      }

    val expectedRecord =
      AggregatorInternalRecord(
        location = None,
        replicas = List(secondaryLocation)
      )

    it("returns the correct record") {
      result.value shouldBe expectedRecord
    }

    it("stores the replica in the underlying store") {
      val path = ReplicaPath(secondaryLocation.prefix.pathPrefix)

      versionedStore
        .getLatest(path)
        .value
        .identifiedT shouldBe expectedRecord
    }
  }

  describe("handling multiple updates to the same replica") {
    val prefix = createS3ObjectLocationPrefix

    val location1 = SecondaryS3ReplicaLocation(
      prefix = prefix.copy(bucket = createBucketName)
    )
    val location2 = PrimaryS3ReplicaLocation(
      prefix = prefix.copy(bucket = createBucketName)
    )
    val location3 = SecondaryS3ReplicaLocation(
      prefix = prefix.copy(bucket = createBucketName)
    )

    val locations = Seq(location1, location2, location3)

    val versionedStore =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      )

    val results =
      withAggregator(versionedStore) { aggregator =>
        locations
          .map { loc =>
            aggregator.aggregate(loc)
          }
          .map { _.value }
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
      val path = ReplicaPath(prefix.keyPrefix)

      val expectedRecord =
        AggregatorInternalRecord(
          location = Some(location2),
          replicas = List(location1, location3)
        )

      versionedStore
        .getLatest(path)
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
        .foreach { loc =>
          aggregator.aggregate(loc)
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
      withAggregator(brokenStore)(
        _.aggregate(createPrimaryLocation)
      )

    result.left.value shouldBe UpdateWriteError(err)
  }

  it("accepts adding the same primary location to a record twice") {
    val primaryLocation = createPrimaryLocation

    val expectedRecord =
      AggregatorInternalRecord(
        location = Some(primaryLocation),
        replicas = List.empty
      )

    withAggregator() { aggregator =>
      (1 to 5).map { _ =>
        aggregator
          .aggregate(primaryLocation)
          .value shouldBe expectedRecord
      }
    }
  }

  it("fails if you add different primary locations for the same replica path") {
    val prefix = createS3ObjectLocationPrefix

    val primaryLocation1 = PrimaryS3ReplicaLocation(
      prefix = prefix.copy(bucket = createBucketName)
    )

    val primaryLocation2 = PrimaryS3ReplicaLocation(
      prefix = prefix.copy(bucket = createBucketName)
    )

    withAggregator() { aggregator =>
      val result =
        aggregator.aggregate(primaryLocation1).value

      result shouldBe AggregatorInternalRecord(
        location = Some(primaryLocation1),
        replicas = List()
      )

      val err =
        aggregator.aggregate(primaryLocation2).left.get
      err.e.getMessage should startWith(
        "Record already has a different PrimaryStorageLocation"
      )
    }
  }

  it("only stores unique replica results") {
    val location = createSecondaryLocation

    val results =
      withAggregator() { aggregator =>
        (1 to 3).map { _ =>
          aggregator.aggregate(location)
        }
      }

    val uniqResults = results.map { _.value }.toSet

    uniqResults should have size 1

    uniqResults.head shouldBe AggregatorInternalRecord(location)
  }
}
