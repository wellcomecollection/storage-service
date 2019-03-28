package uk.ac.wellcome.platform.archive.common.config.builders

import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import uk.ac.wellcome.monitoring.typesafe.MetricsBuilder
import uk.ac.wellcome.platform.archive.common.operation.services.DiagnosticReporter

import scala.concurrent.ExecutionContext

object DiagnosticReporterBuilder {
  def build(config: Config)(
    implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext): DiagnosticReporter =
    new DiagnosticReporter(
      metricsSender = MetricsBuilder.buildMetricsSender(config)
    )
}