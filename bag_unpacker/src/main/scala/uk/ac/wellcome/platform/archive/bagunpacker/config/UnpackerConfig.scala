package uk.ac.wellcome.platform.archive.bagunpacker.config

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class UnpackerConfig(
  dstNamespace: String,
  maybeDstPrefix: Option[String] = None
)

object UnpackerConfig {
  def apply(config: Config): UnpackerConfig = {
    val namespace = config.required[String]("destination.namespace")
    val prefix = config.get[String]("destination.prefix")

    UnpackerConfig(namespace, prefix)
  }
}
