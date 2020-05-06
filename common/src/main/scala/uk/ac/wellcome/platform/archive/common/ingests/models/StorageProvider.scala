package uk.ac.wellcome.platform.archive.common.ingests.models

sealed trait StorageProvider {
  val id: String
}

case object StorageProvider {
  private val idLookup = Map(
    StandardStorageProvider.id -> StandardStorageProvider,
    InfrequentAccessStorageProvider.id -> InfrequentAccessStorageProvider,
    GlacierStorageProvider.id -> GlacierStorageProvider
  )

  def allowedValues: Seq[String] =
    idLookup.keys.toSeq

  def apply(id: String): StorageProvider =
    idLookup.get(id) match {
      case Some(provider) => provider
      case None =>
        throw new IllegalArgumentException(
          s"Unrecognised storage provider ID: $id; valid values are: ${allowedValues.mkString(", ")}"
        )
    }
}

case object StandardStorageProvider extends StorageProvider {
  override val id: String = "aws-s3-standard"
}

case object InfrequentAccessStorageProvider extends StorageProvider {
  override val id: String = "aws-s3-ia"
}

case object GlacierStorageProvider extends StorageProvider {
  override val id: String = "aws-s3-glacier"
}
