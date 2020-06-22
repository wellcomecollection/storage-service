package uk.ac.wellcome.platform.archive.common.config.builders

import java.net.URL

import com.typesafe.config.Config
import uk.ac.wellcome.platform.archive.common.config.models.HTTPServerConfig
import uk.ac.wellcome.typesafe.config.builders.EnrichConfig._

object HTTPServerBuilder {
  def buildHTTPServerConfig(config: Config): HTTPServerConfig = {
    val host = config.requireString("http.host")
    val port = config.requireInt("http.port")
    val externalBaseURL = config.requireString("http.externalBaseURL")

    HTTPServerConfig(
      host = host,
      port = port,
      externalBaseURL = externalBaseURL
    )
  }

  def buildContextURL(config: Config): URL =
    new URL(config.requireString("contextURL"))
}
