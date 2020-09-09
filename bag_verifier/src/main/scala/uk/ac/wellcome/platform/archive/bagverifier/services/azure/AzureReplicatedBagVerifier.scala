package uk.ac.wellcome.platform.archive.bagverifier.services.azure

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.azure.AzureFixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.services.ReplicatedBagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureResolvable
import uk.ac.wellcome.platform.archive.common.bagit.services.azure.AzureBagReader
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.listing.azure.AzureBlobLocationListing
import uk.ac.wellcome.storage.store.azure.AzureStreamStore

class AzureReplicatedBagVerifier(val primaryBucket: String)(
  implicit val s3Client: AmazonS3,
  implicit val blobClient: BlobServiceClient
) extends ReplicatedBagVerifier[AzureBlobLocation, AzureBlobLocationPrefix] {

  override implicit val bagReader: AzureBagReader = new AzureBagReader()

  override implicit val listing: AzureBlobLocationListing =
    AzureBlobLocationListing()

  override implicit val resolvable: AzureResolvable = new AzureResolvable()

  override implicit val fixityChecker: AzureFixityChecker =
    new AzureFixityChecker()

  override def getRelativePath(
    root: AzureBlobLocationPrefix,
    location: AzureBlobLocation
  ): String =
    location.name.replace(root.namePrefix, "")

  override val replicaReader: AzureStreamStore = new AzureStreamStore()
}
