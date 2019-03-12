package uk.ac.wellcome.platform.archive.bagunpacker.config

import com.typesafe.config.Config
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

case class UnpackerConfig(
  bufferSize: Int = UnpackerConfig.DEFAULT_BUFFER_SIZE,
)

object UnpackerConfig {
  val DEFAULT_BUFFER_SIZE = 8192

  def apply(config: Config): UnpackerConfig = {
    val bufferSizeString = config.get[String]("unpacker.buffer.size")
    val bufferSize = bufferSizeString match {
      case Some(str) => Integer.parseInt(str)
      case None => DEFAULT_BUFFER_SIZE
    }
    UnpackerConfig(bufferSize)
  }
}
