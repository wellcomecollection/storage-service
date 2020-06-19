package uk.ac.wellcome.platform.archive.bagunpacker.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.BagUnpackerWorkerConfig
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object UnpackerWorkerConfigBuilder {
  def build(config: Config): BagUnpackerWorkerConfig = {
    val namespace = config.requireString("destination.namespace")
    BagUnpackerWorkerConfig(namespace)
  }
}
