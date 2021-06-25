package weco.storage_service.bag_unpacker.config.builders

import com.typesafe.config.Config
import weco.storage_service.bag_unpacker.config.models.BagUnpackerWorkerConfig
import weco.typesafe.config.builders.EnrichConfig._

object UnpackerWorkerConfigBuilder {
  def build(config: Config): BagUnpackerWorkerConfig = {
    val namespace = config.requireString("destination.namespace")
    BagUnpackerWorkerConfig(namespace)
  }
}
