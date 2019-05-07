package uk.ac.wellcome.platform.archive.common.bagit.models

import java.net.URI

case class FetchEntry(
  url: URI,
  length: Option[Int],
  filepath: String
)
