package uk.ac.wellcome.platform.archive.display
import java.util.UUID

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest

case class DisplayIngestMinimal(
  id: UUID,
  createdDate: String,
  @JsonKey("type") ontologyType: String = "Ingest"
)

case object DisplayIngestMinimal {
  def apply(ingest: Ingest): DisplayIngestMinimal =
    DisplayIngestMinimal(
      id = ingest.id.underlying,
      createdDate = ingest.createdDate.toString
    )
}
