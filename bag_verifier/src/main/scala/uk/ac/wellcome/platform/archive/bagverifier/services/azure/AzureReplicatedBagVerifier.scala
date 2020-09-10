package uk.ac.wellcome.platform.archive.bagverifier.services.azure

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityListChecker
import uk.ac.wellcome.platform.archive.bagverifier.fixity.azure.AzureFixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.services.ReplicatedBagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureResolvable
import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
import uk.ac.wellcome.platform.archive.common.bagit.services.azure.AzureBagReader
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.listing.azure.AzureBlobLocationListing
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.azure.AzureStreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore

class AzureReplicatedBagVerifier(val primaryBucket: String,
                                  val bagReader: AzureBagReader,
                                  val listing: AzureBlobLocationListing,
                                  val resolvable: AzureResolvable,
                                 val fixityListChecker: FixityListChecker[AzureBlobLocation, AzureBlobLocationPrefix, Bag],
                                  val replicaReader: AzureStreamStore,
                                 val srcStreamStore: StreamStore[S3ObjectLocation]
                                )
   extends ReplicatedBagVerifier[AzureBlobLocation, AzureBlobLocationPrefix] {


  override def getRelativePath(
    root: AzureBlobLocationPrefix,
    location: AzureBlobLocation
  ): String =
    location.name.replace(root.namePrefix, "")
}

object AzureReplicatedBagVerifier{
  def apply(primaryBucket: String,
  dynamoConfig: DynamoConfig)(
    implicit s3Client: AmazonS3,
   blobClient: BlobServiceClient,
  dynamoClient: AmazonDynamoDB): AzureReplicatedBagVerifier = {
    val bagReader = new AzureBagReader()
    val listing = AzureBlobLocationListing()
    val resolvable = new AzureResolvable()
    implicit val fixityChecker = new AzureFixityChecker(dynamoConfig)
    implicit val fetchDirectoryFixityChecker = new S3FixityChecker()
    val replicaReader = new AzureStreamStore()
    val srcStreamStore = new S3StreamStore()
    val fixityListChecker = new FixityListChecker[AzureBlobLocation, AzureBlobLocationPrefix, Bag]()
    new AzureReplicatedBagVerifier(primaryBucket, bagReader, listing, resolvable, fixityListChecker, replicaReader,srcStreamStore)
  }
}