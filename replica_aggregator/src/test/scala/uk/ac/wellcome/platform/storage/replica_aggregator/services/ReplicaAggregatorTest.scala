package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.{UpdateWriteError, Version}
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

class ReplicaAggregatorTest
    extends FunSpec
    with Matchers
    with EitherValues
    with TryValues
    with ObjectLocationGenerators
    with StorageRandomThings {

  def withAggregator[R](
    versionedStore: MemoryVersionedStore[String, Set[ReplicaResult]] =
      MemoryVersionedStore[String, Set[ReplicaResult]](initialEntries = Map.empty))(
    testWith: TestWith[ReplicaAggregator, R]): R =
    testWith(
      new ReplicaAggregator(versionedStore)
    )

  it("completes after a single primary replica") {
    val location = createObjectLocation

    val replicaResult = ReplicaResult(
      ingestId = createIngestID,
      storageLocation = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = location
      ),
      timestamp = Instant.now
    )

    val result =
      withAggregator() {
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
      storageLocation = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = createObjectLocation
      ),
      timestamp = Instant.now
    )

    val versionedStore =
      MemoryVersionedStore[String, Set[ReplicaResult]](initialEntries = Map.empty)

    withAggregator(versionedStore) {
      _.aggregate(replicaResult)
    }

    val path = replicaResult.storageLocation.location.path

    versionedStore.getLatest(path).right.value.identifiedT shouldBe Set(replicaResult)
  }

  it("errors if asked to aggregate a secondary replica") {
    val replicaResult = ReplicaResult(
      ingestId = createIngestID,
      storageLocation = SecondaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = createObjectLocation
      ),
      timestamp = Instant.now
    )

    val result =
      withAggregator() {
        _.aggregate(replicaResult)
      }

    val err = result.failed.get
    err.getMessage shouldBe s"Not yet supported! Cannot aggregate secondary replica result: $replicaResult"
  }

  it("handles an error from the underlying versioned store") {
    val throwable = new Throwable("BOOM!")

    val brokenStore = new MemoryVersionedStore[String, Set[ReplicaResult]](
      store = new MemoryStore[Version[String, Int], Set[ReplicaResult]](initialEntries = Map.empty)
        with MemoryMaxima[String, Set[ReplicaResult]]
    ) {
      override def upsert(id: String)(t: Set[ReplicaResult])(f: Set[ReplicaResult] => Set[ReplicaResult]): UpdateEither =
        Left(UpdateWriteError(throwable))
    }

    val replicaResult = ReplicaResult(
      ingestId = createIngestID,
      storageLocation = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = createObjectLocation
      ),
      timestamp = Instant.now
    )

    val result =
      withAggregator(brokenStore) {
        _.aggregate(replicaResult)
      }

    val summary = result.success.value

    summary shouldBe a[ReplicationAggregationFailed]
    val failure = summary.asInstanceOf[ReplicationAggregationFailed]
    failure.e shouldBe throwable
  }

  // duplicates ignored
}
