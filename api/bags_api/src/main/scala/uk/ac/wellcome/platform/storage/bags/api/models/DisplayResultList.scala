package uk.ac.wellcome.platform.storage.bags.api.models

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest

case class ResultListEntry(
  id: String,
  version: String,
  createdDate: String,
  @JsonKey("type") ontologyType: String = "Bag"
)

case object ResultListEntry {
  def apply(manifest: StorageManifest): ResultListEntry =
    ResultListEntry(
      id = manifest.id.toString,
      version = s"v${manifest.version}",
      createdDate = manifest.createdDate.toString
    )
}

case class DisplayResultList(
  @JsonKey("@context") context: String,
  results: Seq[ResultListEntry],
  @JsonKey("type") ontologyType: String = "ResultList"
)
