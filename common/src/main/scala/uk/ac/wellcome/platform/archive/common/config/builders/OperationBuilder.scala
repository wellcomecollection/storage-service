package uk.ac.wellcome.platform.archive.common.config.builders

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.monitoring.typesafe.MetricsSenderBuilder
import uk.ac.wellcome.platform.archive.common.ingests.services.IngestUpdater
import uk.ac.wellcome.platform.archive.common.operation.services.{DiagnosticReporter, OutgoingPublisher}

object OperationBuilder {

  def buildIngestUpdater(config: Config, operationName: String): IngestUpdater =
    new IngestUpdater(
      operationName = operationName,
      snsWriter = SNSBuilder.buildSNSWriter(
        config = config,
        namespace = "ingest"
      )
    )

  def buildOutgoingPublisher(config: Config,
                             operationName: String): OutgoingPublisher =
    new OutgoingPublisher(
      operationName = operationName,
      snsWriter = SNSBuilder.buildSNSWriter(
        config = config,
        namespace = "outgoing"
      )
    )

  def buildOperationReporter(config: Config)(
    implicit actorSystem: ActorSystem): DiagnosticReporter =
    new DiagnosticReporter(
      metricsSender = MetricsSenderBuilder.buildMetricsSender(config)
    )
}
