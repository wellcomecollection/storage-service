package uk.ac.wellcome.platform.archive.bagunpacker.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.UnpackerConfig
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object UnpackerConfigBuilder {
  def build(config: Config): UnpackerConfig = {
    val DEFAULT_BUFFER_SIZE = 8192

    val bufferSize =
      config.getOrElse[Int]("unpacker.buffer.size")(DEFAULT_BUFFER_SIZE)

    UnpackerConfig(
      bufferSize = bufferSize
    )
  }
}
