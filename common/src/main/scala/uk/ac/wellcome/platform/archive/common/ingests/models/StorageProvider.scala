package uk.ac.wellcome.platform.archive.common.ingests.models

sealed trait StorageProvider {
  val id: String
}

case object StorageProvider {
  private val idLookup = Map(
    AmazonS3StorageProvider.id -> AmazonS3StorageProvider,
    AzureBlobStorageProvider.id -> AzureBlobStorageProvider
  )

  // This map includes the identifiers of storage providers that we have
  // removed from the storage service.  We still need to be able to deserialise
  // them for backwards-compatibility with clients, ingests and storage manifests,
  // but we don't want to advertise their presence in, say, error messages.
  private val deprecatedIdLookup = Map(
    "aws-s3-standard" -> AmazonS3StorageProvider,
    "aws-s3-ia" -> AmazonS3StorageProvider,
    "aws-s3-glacier" -> AmazonS3StorageProvider
  )

  def recognisedValues: Seq[String] =
    idLookup.keys.toSeq

  def allowedValues: Seq[String] =
    (idLookup ++ deprecatedIdLookup).keys.toSeq

  def apply(id: String): StorageProvider =
    (idLookup ++ deprecatedIdLookup).get(id) match {
      case Some(provider) => provider
      case None =>
        throw new IllegalArgumentException(
          s"Unrecognised storage provider ID: $id; valid values are: ${recognisedValues.mkString(", ")}"
        )
    }
}

case object AmazonS3StorageProvider extends StorageProvider {
  override val id: String = "amazon-s3"
}

case object AzureBlobStorageProvider extends StorageProvider {
  override val id: String = "azure-blob-storage"
}

