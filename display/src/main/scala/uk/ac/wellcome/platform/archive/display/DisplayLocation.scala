package uk.ac.wellcome.platform.archive.display

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.ingests.models.SourceLocation
import uk.ac.wellcome.platform.archive.common.storage.models.StorageLocation
import uk.ac.wellcome.storage.ObjectLocation

case class DisplayLocation(
  provider: DisplayProvider,
  bucket: String,
  path: String,
  @JsonKey("type") ontologyType: String = "Location"
) {
  def toSourceLocation: SourceLocation =
    SourceLocation(
      provider = provider.toStorageProvider,
      location = ObjectLocation(bucket, path)
    )
}

object DisplayLocation {
  def apply(location: SourceLocation): DisplayLocation =
    DisplayLocation(
      provider = DisplayProvider(location.provider),
      bucket = location.location.namespace,
      path = location.location.path
    )

  def apply(location: StorageLocation): DisplayLocation =
    DisplayLocation(
      provider = DisplayProvider(location.provider),
      bucket = location.location.namespace,
      path = location.location.path
    )
}
