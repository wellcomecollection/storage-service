package uk.ac.wellcome.platform.archive.bagunpacker.config.models

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class UnpackerWorkerConfig(
  dstNamespace: String,
  maybeDstPrefix: Option[String] = None
)

object UnpackerWorkerConfig {
  def apply(config: Config): UnpackerWorkerConfig = {
    val namespace = config.required[String]("destination.namespace")
    val prefix = config.get[String]("destination.prefix")
    UnpackerWorkerConfig(namespace, prefix)
  }
}
