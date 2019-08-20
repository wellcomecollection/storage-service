package uk.ac.wellcome.platform.storage.replica_aggregator.services
import uk.ac.wellcome.platform.storage.replica_aggregator.models.{AggregatorInternalRecord, KnownReplicas}

sealed trait ReplicaCounterError

case class NoPrimaryReplica() extends ReplicaCounterError

case class NotEnoughReplicas(
  expected: Int,
  actual: Int
) extends ReplicaCounterError

/** Check whether we have enough replicas to mark an ingest as "complete".
  *
  * It checks that:
  *
  *   - there's a primary replica
  *   - there are at least `expectedReplicas` overall
  *
  */
class ReplicaCounter(expectedReplicaCount: Int) {
  def countReplicas(record: AggregatorInternalRecord): Either[ReplicaCounterError, KnownReplicas] =
    record.location match {
      case None => Left(NoPrimaryReplica())

      case Some(primaryLocation) =>
        // All the secondary replicas + 1 for the primary
        val actualReplicaCount = record.replicas.size + 1

        if (actualReplicaCount >= expectedReplicaCount) {
          Right(
            KnownReplicas(
              location = primaryLocation,
              replicas = record.replicas
            )
          )
        } else {
          Left(
            NotEnoughReplicas(
              expected = expectedReplicaCount,
              actual = actualReplicaCount
            )
          )
        }
    }
}
