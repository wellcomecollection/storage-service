package uk.ac.wellcome.platform.archive.bagverifier.services.azure

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.services.ReplicatedBagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureResolvable
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.azure.AzureBagReader
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.azure.AzureBlobLocationListing
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.azure.AzureStreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore

class AzureReplicatedBagVerifier(val bucket: String)(
  implicit val s3Client: AmazonS3,
  implicit val blobClient: BlobServiceClient
) extends ReplicatedBagVerifier[AzureBlobLocation, AzureBlobLocationPrefix] {

  override val srcStreamStore: StreamStore[S3ObjectLocation] =
    new S3StreamStore()
  override implicit val bagReader: BagReader[AzureBlobLocation, AzureBlobLocationPrefix] = new AzureBagReader()
  override implicit val listing: Listing[AzureBlobLocationPrefix, AzureBlobLocation] = AzureBlobLocationListing()
  override implicit val resolvable: Resolvable[AzureBlobLocation] = new AzureResolvable()
  override implicit val fixityChecker: FixityChecker[AzureBlobLocation, AzureBlobLocationPrefix] = ???

  override def getRelativePath(root: AzureBlobLocationPrefix, location: AzureBlobLocation): String = location.name.replace(root.namePrefix, "")

  override val replicaStreamStore: StreamStore[AzureBlobLocation] = new AzureStreamStore()
}