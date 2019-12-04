package uk.ac.wellcome.platform.storage.ingests.api.responses

import java.net.URL

import io.circe.Printer

trait ResponseBase {
  val contextURL: URL

  implicit val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)
}
