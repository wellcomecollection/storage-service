package weco.storage_service.config.builders

import com.typesafe.config.Config
import weco.typesafe.config.builders.EnrichConfig._

object OperationNameBuilder {
  def getName(config: Config): String =
    config.requireString("operation.name")
}
