package uk.ac.wellcome.platform.archive.common.bagit.models

import java.net.URL

case class FetchEntry(
  url: URL,
  length: Option[Int],
  filepath: String
)
