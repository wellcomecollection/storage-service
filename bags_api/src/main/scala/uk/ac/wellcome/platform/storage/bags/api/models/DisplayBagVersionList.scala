package uk.ac.wellcome.platform.storage.bags.api.models

import java.net.URL

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.bag_tracker.models.BagVersionList

case class DisplayBagVersionList(
  @JsonKey("@context") context: String,
  results: Seq[DisplayBagVersionEntry],
  @JsonKey("type") ontologyType: String = "ResultList"
)

case object DisplayBagVersionList {
  def apply(
    contextURL: URL,
    bagVersionList: BagVersionList
  ): DisplayBagVersionList =
    DisplayBagVersionList(
      context = contextURL.toString,
      results = bagVersionList.versions.map { entry =>
        DisplayBagVersionEntry(
          id = bagVersionList.id,
          entry = entry
        )
      }
    )
}
