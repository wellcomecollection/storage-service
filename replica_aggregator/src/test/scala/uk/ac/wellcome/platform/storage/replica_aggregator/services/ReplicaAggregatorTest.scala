package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{
  StorageLocation,
  PrimaryStorageLocation,
  SecondaryStorageLocation
}
import uk.ac.wellcome.platform.storage.replica_aggregator.models._
import uk.ac.wellcome.storage.{UpdateWriteError, Version}
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

import scala.collection.immutable
import scala.util.Try

class ReplicaAggregatorTest
    extends FunSpec
    with Matchers
    with EitherValues
    with TryValues
    with ObjectLocationGenerators
    with StorageRandomThings {

  def createReplicaResultWith(
    storageLocation: StorageLocation = PrimaryStorageLocation(
      provider = InfrequentAccessStorageProvider,
      location = createObjectLocation
    )
  ): ReplicaResult =
    ReplicaResult(
      ingestId = createIngestID,
      storageLocation = storageLocation,
      timestamp = Instant.now
    )

  def createReplicaResult: ReplicaResult =
    createReplicaResultWith()

  def withAggregator[R](
    versionedStore: MemoryVersionedStore[ReplicaPath, List[ReplicaResult]] =
      MemoryVersionedStore[ReplicaPath, List[ReplicaResult]](
        initialEntries = Map.empty
      )
  )(testWith: TestWith[ReplicaAggregator, R]): R =
    testWith(
      new ReplicaAggregator(versionedStore)
    )

  it("completes after a single primary replica") {
    val location = createObjectLocation

    val replicaResult = createReplicaResultWith(
      storageLocation = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = location
      )
    )

    val result =
      withAggregator() {
        _.aggregate(replicaResult)
      }

    val summary = result.success.value

    summary shouldBe a[ReplicationAggregationComplete]
    summary.asInstanceOf[ReplicationAggregationComplete].replicationSet shouldBe
      ReplicationSet(
        path = ReplicaPath(location.path),
        results = List(replicaResult)
      )
  }

  it("stores a single primary replica") {
    val replicaResult = createReplicaResultWith(
      storageLocation = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = createObjectLocation
      )
    )

    val versionedStore =
      MemoryVersionedStore[ReplicaPath, List[ReplicaResult]](
        initialEntries = Map.empty
      )

    withAggregator(versionedStore) {
      _.aggregate(replicaResult)
    }

    val path = ReplicaPath(replicaResult.storageLocation.location.path)

    versionedStore.getLatest(path).right.value.identifiedT shouldBe List(
      replicaResult
    )
  }

  it("errors if asked to aggregate a secondary replica") {
    val replicaResult = createReplicaResultWith(
      storageLocation = SecondaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        location = createObjectLocation
      )
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

    val brokenStore =
      new MemoryVersionedStore[ReplicaPath, List[ReplicaResult]](
        store = new MemoryStore[Version[ReplicaPath, Int], List[ReplicaResult]](
          initialEntries = Map.empty
        ) with MemoryMaxima[ReplicaPath, List[ReplicaResult]]
      ) {
        override def upsert(id: ReplicaPath)(
          t: List[ReplicaResult]
        )(f: List[ReplicaResult] => List[ReplicaResult]): UpdateEither =
          Left(UpdateWriteError(throwable))
      }

    val result =
      withAggregator(brokenStore) {
        _.aggregate(createReplicaResult)
      }

    val summary = result.success.value

    summary shouldBe a[ReplicationAggregationFailed]
    val failure = summary.asInstanceOf[ReplicationAggregationFailed]
    failure.e shouldBe throwable
  }

  it("only stores unique replica results") {
    val replicaResult = createReplicaResult

    val results: immutable.Seq[Try[ReplicationAggregationSummary]] =
      withAggregator() { aggregator =>
        (1 to 3).map { _ =>
          aggregator.aggregate(replicaResult)
        }
      }

    results.foreach { result =>
      val summary = result.success.value

      summary shouldBe a[ReplicationAggregationComplete]
      summary
        .asInstanceOf[ReplicationAggregationComplete]
        .replicationSet shouldBe
        ReplicationSet(
          path = ReplicaPath(replicaResult.storageLocation.location.path),
          results = List(replicaResult)
        )
    }
  }

  // TODO (separate patch):
  //
  // *  It deduplicates replicas to the same location, even if they have
  //    different timestamps
  // *  It returns an "incomplete" summary if we don't have a primary replica
  //    and/or we don't have enough secondary replicas
  // *  It errors if it gets multiple primary replicas
  //
}
