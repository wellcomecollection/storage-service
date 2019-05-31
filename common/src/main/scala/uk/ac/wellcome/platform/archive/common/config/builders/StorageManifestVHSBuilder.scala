package uk.ac.wellcome.platform.archive.common.config.builders

import com.typesafe.config.Config
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
import uk.ac.wellcome.storage.streaming.CodecInstances._
import uk.ac.wellcome.storage.typesafe.VHSBuilder
import uk.ac.wellcome.storage.vhs.EmptyMetadata

object StorageManifestVHSBuilder {
  def build(config: Config): StorageManifestDao =
    new StorageManifestDao(
      underlying =
        VHSBuilder.buildVHS[String, StorageManifest, EmptyMetadata](config)
    )
}
