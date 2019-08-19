package uk.ac.wellcome.platform.storage.replica_aggregator.services

import java.time.Instant

import org.scalatest.{EitherValues, FunSpec, Matchers, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.ingests.models.InfrequentAccessStorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.{
  PrimaryStorageLocation,
  SecondaryStorageLocation,
  StorageLocation
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
    versionedStore: MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord] =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      )
  )(testWith: TestWith[ReplicaAggregator, R]): R =
    testWith(
      new ReplicaAggregator(versionedStore)
    )

  it("completes after a single primary replica") {
    val prefix = createObjectLocationPrefix

    val replicaResult = createReplicaResultWith(
      storageLocation = PrimaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        prefix = prefix
      )
    )

    val result =
      withAggregator() {
        _.aggregate(replicaResult)
      }

    val summary = result.success.value

    summary shouldBe a[ReplicationAggregationComplete]

    val complete = summary.asInstanceOf[ReplicationAggregationComplete]
    complete.replicaPath shouldBe ReplicaPath(prefix.path)
    complete.aggregatorRecord shouldBe AggregatorInternalRecord(replicaResult.storageLocation)
  }

  it("stores a single primary replica") {
    val storageLocation = PrimaryStorageLocation(
      provider = InfrequentAccessStorageProvider,
      prefix = createObjectLocationPrefix
    )

    val replicaResult = createReplicaResultWith(
      storageLocation = storageLocation
    )

    val versionedStore =
      MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        initialEntries = Map.empty
      )

    withAggregator(versionedStore) {
      _.aggregate(replicaResult)
    }

    val path = ReplicaPath(replicaResult.storageLocation.prefix.path)

    versionedStore.getLatest(path).right.value.identifiedT shouldBe AggregatorInternalRecord(
       location = Some(storageLocation),
       replicas = List.empty
     )
  }

  it("errors if asked to aggregate a secondary replica") {
    val replicaResult = createReplicaResultWith(
      storageLocation = SecondaryStorageLocation(
        provider = InfrequentAccessStorageProvider,
        prefix = createObjectLocationPrefix
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
      new MemoryVersionedStore[ReplicaPath, AggregatorInternalRecord](
        store = new MemoryStore[Version[ReplicaPath, Int], AggregatorInternalRecord](
          initialEntries = Map.empty
        ) with MemoryMaxima[ReplicaPath, AggregatorInternalRecord]
      ) {
        override def upsert(id: ReplicaPath)(
          t: AggregatorInternalRecord
        )(f: AggregatorInternalRecord => AggregatorInternalRecord): UpdateEither =
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

      val complete = summary.asInstanceOf[ReplicationAggregationComplete]
      complete.aggregatorRecord shouldBe AggregatorInternalRecord(replicaResult.storageLocation)
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
