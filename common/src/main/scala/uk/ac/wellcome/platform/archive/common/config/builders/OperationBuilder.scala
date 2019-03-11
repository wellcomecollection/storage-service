package uk.ac.wellcome.platform.archive.common.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.messaging.typesafe.SNSBuilder
import uk.ac.wellcome.platform.archive.common.operation.{IngestNotifier, OperationNotifier, OutgoingNotifier}

object OperationBuilder {

  def buildIngestNotifier(config: Config, operationName: String, namespace: String = ""): IngestNotifier =
    new IngestNotifier(
      operationName = operationName,
      snsWriter = SNSBuilder.buildSNSWriter(
        config = config,
        namespace = namespace
      )
    )

  def buildOutgoingNotifier(config: Config, operationName: String, namespace: String = ""): OutgoingNotifier =
    new OutgoingNotifier(
      operationName = operationName,
      snsWriter = SNSBuilder.buildSNSWriter(
        config = config,
        namespace = namespace
      )
    )

  def build(config: Config, operationName: String): OperationNotifier =
    new OperationNotifier(
      outgoing = buildOutgoingNotifier(
        config, operationName, namespace = "outgoing"
      ),
      ingests = buildIngestNotifier(
        config, operationName, namespace = "progress"
      )
    )
}
