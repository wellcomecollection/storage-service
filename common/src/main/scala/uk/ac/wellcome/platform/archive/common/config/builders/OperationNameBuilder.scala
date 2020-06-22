package uk.ac.wellcome.platform.archive.common.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object OperationNameBuilder {
  def getName(config: Config): String =
    config.requireString("operation.name")
}
