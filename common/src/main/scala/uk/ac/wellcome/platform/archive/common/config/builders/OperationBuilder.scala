package uk.ac.wellcome.platform.archive.common.config.builders

import akka.actor.ActorSystem
import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.monitoring.typesafe.MetricsSenderBuilder
import uk.ac.wellcome.platform.archive.common.operation.{
  IngestNotifier,
  OperationNotifier,
  OperationReporter,
  OutgoingNotifier
}

object OperationBuilder {

  def buildIngestNotifier(config: Config,
                          operationName: String,
                          namespace: String = ""): IngestNotifier =
    new IngestNotifier(
      operationName = operationName,
      snsWriter = SNSBuilder.buildSNSWriter(
        config = config,
        namespace = namespace
      )
    )

  def buildOutgoingNotifier(config: Config,
                            operationName: String,
                            namespace: String = ""): OutgoingNotifier =
    new OutgoingNotifier(
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
      ingests = buildIngestNotifier(
        config,
        operationName,
        namespace = "progress"
      )
    )

  def buildOperationReporter(config: Config)(
    implicit actorSystem: ActorSystem): OperationReporter =
    new OperationReporter(
      metricsSender = MetricsSenderBuilder.buildMetricsSender(config)
    )
}
