package uk.ac.wellcome.platform.storage.bags.api.models

import java.net.URL

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.display.{
  DisplayLocation,
  DisplayStorageSpace
}

case class ResponseDisplayBag(
  @JsonKey("@context") context: String,
  id: String,
  space: DisplayStorageSpace,
  info: ResponseDisplayBagInfo,
  manifest: DisplayFileManifest,
  tagManifest: DisplayFileManifest,
  locations: Seq[DisplayLocation],
  createdDate: String,
  @JsonKey("type") ontologyType: String = "Bag"
)

object ResponseDisplayBag {
  def apply(storageManifest: StorageManifest, contextUrl: URL): ResponseDisplayBag =
    ResponseDisplayBag(
      context = contextUrl.toString,
      id = storageManifest.id.toString,
      space = DisplayStorageSpace(storageManifest.space.underlying),
      info = ResponseDisplayBagInfo(storageManifest.info),
      manifest = DisplayFileManifest(storageManifest.manifest),
      tagManifest = DisplayFileManifest(storageManifest.tagManifest),
      locations = storageManifest.locations.map { DisplayLocation(_) },
      createdDate = storageManifest.createdDate.toString
    )
}
