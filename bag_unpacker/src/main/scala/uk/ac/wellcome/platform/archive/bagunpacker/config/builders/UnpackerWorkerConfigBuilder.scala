package uk.ac.wellcome.platform.archive.bagunpacker.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.UnpackerWorkerConfig
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object UnpackerWorkerConfigBuilder {
  def build(config: Config): UnpackerWorkerConfig = {
    val namespace = config.required[String]("destination.namespace")
    val prefix = config.get[String]("destination.prefix")
    UnpackerWorkerConfig(namespace, prefix)
  }
}
