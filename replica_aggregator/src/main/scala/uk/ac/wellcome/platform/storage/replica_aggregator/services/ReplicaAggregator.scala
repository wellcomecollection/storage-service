package uk.ac.wellcome.platform.storage.replica_aggregator.services

import uk.ac.wellcome.platform.storage.replica_aggregator.models.{
  ReplicaResult,
  ReplicationAggregationSummary
}

import scala.util.Try

class ReplicaAggregator {
  def aggregate(result: ReplicaResult): Try[ReplicationAggregationSummary] = ???
}
