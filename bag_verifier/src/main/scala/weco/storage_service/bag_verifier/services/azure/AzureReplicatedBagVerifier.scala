package weco.storage_service.bag_verifier.services.azure

import com.azure.storage.blob.BlobServiceClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.s3.S3Client
import weco.storage_service.bag_verifier.fixity.FixityListChecker
import weco.storage_service.bag_verifier.fixity.azure.AzureFixityChecker
import weco.storage_service.bag_verifier.fixity.s3.S3FixityChecker
import weco.storage_service.bag_verifier.services.ReplicatedBagVerifier
import weco.storage_service.bag_verifier.storage.Resolvable
import weco.storage_service.bag_verifier.storage.azure.AzureResolvable
import weco.storage_service.bagit.models.Bag
import weco.storage_service.bagit.services.BagReader
import weco.storage_service.bagit.services.azure.AzureBagReader
import weco.storage.providers.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import weco.storage.dynamo.DynamoConfig
import weco.storage.listing.Listing
import weco.storage.listing.azure.AzureBlobLocationListing
import weco.storage.store.StreamStore
import weco.storage.store.azure.AzureStreamStore
import weco.storage.store.s3.S3StreamReader

class AzureReplicatedBagVerifier(
  val primaryBucket: String,
  val bagReader: BagReader[AzureBlobLocation, AzureBlobLocationPrefix],
  val listing: Listing[AzureBlobLocationPrefix, AzureBlobLocation],
  val resolvable: Resolvable[AzureBlobLocation],
  val fixityListChecker: FixityListChecker[
    AzureBlobLocation,
    AzureBlobLocationPrefix,
    Bag
  ],
  val srcReader: S3StreamReader,
  val streamReader: StreamStore[AzureBlobLocation]
) extends ReplicatedBagVerifier[AzureBlobLocation, AzureBlobLocationPrefix] {

  override def getRelativePath(
    root: AzureBlobLocationPrefix,
    location: AzureBlobLocation
  ): String =
    location.name.replace(root.namePrefix, "")
}

object AzureReplicatedBagVerifier {
  def apply(primaryBucket: String, dynamoConfig: DynamoConfig)(
    implicit s3Client: S3Client,
    blobClient: BlobServiceClient,
    dynamoClient: DynamoDbClient
  ): AzureReplicatedBagVerifier = {
    val bagReader = new AzureBagReader()
    val listing = AzureBlobLocationListing()
    val resolvable = new AzureResolvable()
    implicit val fixityChecker = AzureFixityChecker(dynamoConfig)
    implicit val fetchDirectoryFixityChecker = S3FixityChecker()
    val fixityListChecker =
      new FixityListChecker[AzureBlobLocation, AzureBlobLocationPrefix, Bag]()
    new AzureReplicatedBagVerifier(
      primaryBucket,
      bagReader,
      listing,
      resolvable,
      fixityListChecker,
      srcReader = new S3StreamReader(),
      streamReader = new AzureStreamStore()
    )
  }
}
