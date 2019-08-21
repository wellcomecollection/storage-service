package uk.ac.wellcome.platform.archive.common.ingests.models

sealed trait StorageProvider {
  val id: String
}

case object StorageProvider {
  def apply(id: String): StorageProvider =
    id match {
      case StandardStorageProvider.id => StandardStorageProvider
      case InfrequentAccessStorageProvider.id => InfrequentAccessStorageProvider
      case _ =>
        throw new IllegalArgumentException(
          s"Unrecognised storage provider ID: $id"
        )
    }
}

case object StandardStorageProvider extends StorageProvider {
  override val id: String = "aws-s3-standard"
}
case object InfrequentAccessStorageProvider extends StorageProvider {
  override val id: String = "aws-s3-ia"
}
