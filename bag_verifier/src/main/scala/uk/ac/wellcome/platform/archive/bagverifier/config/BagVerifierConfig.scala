package uk.ac.wellcome.platform.archive.bagverifier.config

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class BagVerifierConfig(
  parallelism: Int,
)

object BagVerifierConfig {
  def buildBagVerifierConfig(config: Config): BagVerifierConfig = {
    BagVerifierConfig(
      parallelism =
        config.getOrElse[Int]("bag-verifier.parallelism")(default = 10)
    )
  }
}
