package uk.ac.wellcome.platform.archive.bagverifier.services.azure

import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.services.ReplicatedBagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.azure.AzureBagReader
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.azure.AzureBlobLocationListing
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore

class AzureReplicatedBagVerifier(val primaryBucket: String)(
  implicit val s3Client: AmazonS3,
  implicit val blobClient: BlobServiceClient
) extends ReplicatedBagVerifier[S3ObjectLocation, S3ObjectLocationPrefix,AzureBlobLocation, AzureBlobLocationPrefix] {

  override val srcStreamStore: StreamStore[S3ObjectLocation] =
    new S3StreamStore()
  override implicit val bagReader: BagReader[AzureBlobLocation, AzureBlobLocationPrefix] = new AzureBagReader()
  override implicit val listing: Listing[AzureBlobLocationPrefix, AzureBlobLocation] = AzureBlobLocationListing()
  override implicit val resolvable: Resolvable[S3ObjectLocation] = _
  override implicit val fixityChecker: FixityChecker[S3ObjectLocation] = _

  override def createPrefix(path: String): AzureBlobLocationPrefix = ???

  override def getRelativePath(root: AzureBlobLocationPrefix, location: S3ObjectLocation): String = ???

  override val replicaStreamStore: StreamStore[AzureBlobLocation] = _
}