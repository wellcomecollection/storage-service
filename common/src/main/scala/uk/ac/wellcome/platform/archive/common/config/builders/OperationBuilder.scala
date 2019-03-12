package uk.ac.wellcome.platform.archive.common.config.builders

import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.monitoring.typesafe.MetricsBuilder
import uk.ac.wellcome.platform.archive.common.operation.{
  DiagnosticReporter,
  IngestUpdater,
  OutgoingPublisher
}

import scala.concurrent.ExecutionContext

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
    implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext): DiagnosticReporter =
    new DiagnosticReporter(
      metricsSender = MetricsBuilder.buildMetricsSender(config)
    )
}
