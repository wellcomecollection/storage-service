package uk.ac.wellcome.platform.storage.replica_aggregator.services

import uk.ac.wellcome.platform.archive.common.storage.models.IngestStepResult
import uk.ac.wellcome.platform.storage.replica_aggregator.models.{ReplicaResult, ReplicationAggregationSummary}

import scala.util.Try

class ReplicaAggregator(expectedReplicaCount: Int) {
  def aggregate(result: ReplicaResult): Try[IngestStepResult[ReplicationAggregationSummary]] = ???
}
