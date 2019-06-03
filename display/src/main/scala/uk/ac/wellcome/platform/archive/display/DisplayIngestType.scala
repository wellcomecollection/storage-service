package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey

case class DisplayIngestType(
  id: String,
  @JsonKey("type") ontologyType: String = "IngestType"
)
