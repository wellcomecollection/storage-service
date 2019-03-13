package uk.ac.wellcome.platform.archive.bagunpacker.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.bagunpacker.config.models.UnpackerConfig

object UnpackerConfigBuilder {
  val BUFFER_SIZE_DEFAULT = 8192
  val BUFFER_SIZE_PATH = "unpacker.buffer.size"

  def build(config: Config): UnpackerConfig = {
    UnpackerConfig(
      bufferSize = parseBufferSize(config)
    )
  }

  private def parseBufferSize(config: Config): Int =
    if (config.hasPath(BUFFER_SIZE_PATH)) {
      config.getInt(BUFFER_SIZE_PATH)
    } else {
      BUFFER_SIZE_DEFAULT
    }
}
