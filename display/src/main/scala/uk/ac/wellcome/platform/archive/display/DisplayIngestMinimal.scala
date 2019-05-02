package uk.ac.wellcome.platform.archive.display
import java.util.UUID

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models.BagIngest

case class DisplayIngestMinimal(
  id: UUID,
  createdDate: String,
  @JsonKey("type") ontologyType: String = "Ingest"
)

case object DisplayIngestMinimal {
  def apply(bagIngest: BagIngest): DisplayIngestMinimal =
    DisplayIngestMinimal(
      id = bagIngest.id.underlying,
      createdDate = bagIngest.createdDate.toString
    )
}
