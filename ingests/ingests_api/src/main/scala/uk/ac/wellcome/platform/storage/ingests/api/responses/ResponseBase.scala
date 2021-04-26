package uk.ac.wellcome.platform.storage.ingests.api.responses

import java.net.URL
import io.circe.Printer
import weco.http.json.DisplayJsonUtil

trait ResponseBase {
  val contextURL: URL

  implicit val printer: Printer = DisplayJsonUtil.printer
}
