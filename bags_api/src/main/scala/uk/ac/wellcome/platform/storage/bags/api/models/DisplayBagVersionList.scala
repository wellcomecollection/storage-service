package uk.ac.wellcome.platform.storage.bags.api.models

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.bag_tracker.models.BagVersionList

case class DisplayBagVersionList(
  results: Seq[DisplayBagVersionEntry],
  @JsonKey("type") ontologyType: String = "ResultList"
)

case object DisplayBagVersionList {
  def apply(bagVersionList: BagVersionList): DisplayBagVersionList =
    DisplayBagVersionList(
      results = bagVersionList.versions.map { entry =>
        DisplayBagVersionEntry(
          id = bagVersionList.id,
          entry = entry
        )
      }
    )
}
