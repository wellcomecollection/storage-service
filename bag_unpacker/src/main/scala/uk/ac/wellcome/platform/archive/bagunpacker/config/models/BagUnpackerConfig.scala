package uk.ac.wellcome.platform.archive.bagunpacker.config.models

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class BagUnpackerConfig(
  dstNamespace: String,
  maybeDstPrefix: Option[String] = None
)

object BagUnpackerConfig {
  def apply(config: Config): BagUnpackerConfig = {
    val namespace = config.required[String]("destination.namespace")
    val prefix = config.get[String]("destination.prefix")

    BagUnpackerConfig(namespace, prefix)
  }
}
