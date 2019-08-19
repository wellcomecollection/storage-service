package uk.ac.wellcome.platform.archive.common.ingests.models

sealed trait StorageProvider { val id: String }

case object StandardStorageProvider extends StorageProvider {
  override val id: String = "aws-s3-standard"
}
case object InfrequentAccessStorageProvider extends StorageProvider {
  override val id: String = "aws-s3-ia"
}
