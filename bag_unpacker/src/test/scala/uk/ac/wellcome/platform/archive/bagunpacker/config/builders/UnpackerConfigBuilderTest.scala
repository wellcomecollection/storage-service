package uk.ac.wellcome.platform.archive.bagunpacker.config.builders

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.scalatest.{FunSpec, Matchers}

class UnpackerConfigBuilderTest extends FunSpec with Matchers {

  it("parses a valid bufferSize") {
    val config: Config = ConfigFactory.parseString("unpacker.buffer.size=1")

    val unpackerConfig = UnpackerConfigBuilder.build(config)

    unpackerConfig.bufferSize shouldBe 1
  }

  it("defaults if bufferSize is absent") {
    val config: Config = ConfigFactory.parseString("some.other.config=2")

    val unpackerConfig = UnpackerConfigBuilder.build(config)

    unpackerConfig.bufferSize shouldBe UnpackerConfigBuilder.BUFFER_SIZE_DEFAULT
  }

  it("throws a ConfigException if the type is wrong") {
    val config: Config =
      ConfigFactory.parseString("unpacker.buffer.size=A string")

    assertThrows[ConfigException] {
      UnpackerConfigBuilder.build(config)
    }
  }

  // Typesafe PropertiesParser appears to interpolate an
  // environment variable as a String, so a config with:
  //   unpacker.buffer.size=${?unpacker_buffer_size}
  // is equivalent to
  //   unpacker.buffer.size="1"
  // rather than:
  //   unpacker.buffer.size=1
  // hence this test
  it("parses a number wrapped in a String") {
    val config: Config =
      ConfigFactory.parseString("unpacker.buffer.size=\"12\"")

    val unpackerConfig = UnpackerConfigBuilder.build(config)

    unpackerConfig.bufferSize shouldBe 12
  }
}
