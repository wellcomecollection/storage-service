package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestType

case class DisplayIngestType(
  id: String,
  @JsonKey("type") ontologyType: String = "IngestType"
)

case object DisplayIngestType {
  def apply(ingestType: IngestType): DisplayIngestType =
    DisplayIngestType(id = ingestType.id)
}
