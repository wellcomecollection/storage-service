package uk.ac.wellcome.platform.archive.common.config.builders

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.monitoring.typesafe.MetricsSenderBuilder
import uk.ac.wellcome.platform.archive.common.operation.{
  DiagnosticReporter,
  IngestUpdater,
  OperationNotifier,
  OutgoingPublisher
}

object OperationBuilder {

  def buildIngestNotifier(config: Config,
                          operationName: String,
                          namespace: String = ""): IngestUpdater =
    new IngestUpdater(
      operationName = operationName,
      snsWriter = SNSBuilder.buildSNSWriter(
        config = config,
        namespace = namespace
      )
    )

  def buildOutgoingNotifier(config: Config,
                            operationName: String,
                            namespace: String = ""): OutgoingPublisher =
    new OutgoingPublisher(
      operationName = operationName,
      snsWriter = SNSBuilder.buildSNSWriter(
        config = config,
        namespace = namespace
      )
    )

  def buildOperationNotifier(config: Config,
                             operationName: String): OperationNotifier =
    new OperationNotifier(
      outgoing = buildOutgoingNotifier(
        config,
        operationName,
        namespace = "outgoing"
      ),
      ingestUpdater = buildIngestNotifier(
        config,
        operationName,
        namespace = "progress"
      )
    )

  def buildOperationReporter(config: Config)(
    implicit actorSystem: ActorSystem): DiagnosticReporter =
    new DiagnosticReporter(
      metricsSender = MetricsSenderBuilder.buildMetricsSender(config)
    )
}
