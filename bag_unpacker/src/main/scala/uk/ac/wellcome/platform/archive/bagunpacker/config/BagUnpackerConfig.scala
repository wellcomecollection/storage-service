package uk.ac.wellcome.platform.archive.bagunpacker.config

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class BagUnpackerConfig(
  parallelism: Int,
)

object BagUnpackerConfig {
  def buildBagUnpackerConfig(config: Config): BagUnpackerConfig = {
    BagUnpackerConfig(
      parallelism =
        config.getOrElse[Int]("bag-unpacker.parallelism")(default = 10)
    )
  }
}
