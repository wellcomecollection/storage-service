package uk.ac.wellcome.platform.archive.bagverifier.services.azure

import java.lang.RuntimeException

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.s3.AmazonS3
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityListChecker
import uk.ac.wellcome.platform.archive.bagverifier.fixity.azure.AzureFixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.platform.archive.bagverifier.services.ReplicatedBagVerifier
import uk.ac.wellcome.platform.archive.bagverifier.storage.Resolvable
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureResolvable
import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.azure.AzureBagReader
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.azure.AzureBlobLocationListing
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.s3.S3StreamStore

class AzureReplicatedBagVerifier(val primaryBucket: String,
                                  val bagReader: BagReader[AzureBlobLocation, AzureBlobLocationPrefix],
                                  val listing: Listing[AzureBlobLocationPrefix, AzureBlobLocation],
                                  val resolvable: Resolvable[AzureBlobLocation],
                                 val fixityListChecker: FixityListChecker[AzureBlobLocation, AzureBlobLocationPrefix, Bag],
                                 val srcReader: StreamStore[S3ObjectLocation]
                                )
   extends ReplicatedBagVerifier[AzureBlobLocation, AzureBlobLocationPrefix] {


  override def getRelativePath(
    root: AzureBlobLocationPrefix,
    location: AzureBlobLocation
  ): String =
    location.name.replace(root.namePrefix, "")

  override def isRetriable(error: BagVerifierError): Boolean = error match {
      // When reading from Azure, sometimes we see errors like
    // "StoreReadError(reactor.core.Exceptions$ReactiveException: java.util.concurrent.TimeoutException: Did not
    // observe any item or terminal signal within 60000ms in 'map' (and no fallback has been configured))"
      // These are retriable errors so let's mark them as such
    case BagVerifierError(e: RuntimeException, _) if e.getMessage.contains("TimeoutException") => true
    case _ => false
  }
}

object AzureReplicatedBagVerifier {
  def apply(primaryBucket: String, dynamoConfig: DynamoConfig)(
    implicit s3Client: AmazonS3,
   blobClient: BlobServiceClient,
  dynamoClient: AmazonDynamoDB): AzureReplicatedBagVerifier = {
    val bagReader = AzureBagReader()
    val listing = AzureBlobLocationListing()
    val resolvable = new AzureResolvable()
    implicit val fixityChecker = AzureFixityChecker(dynamoConfig)
    implicit val fetchDirectoryFixityChecker = S3FixityChecker()
    val srcReader = new S3StreamStore()
    val fixityListChecker = new FixityListChecker[AzureBlobLocation, AzureBlobLocationPrefix, Bag]()
    new AzureReplicatedBagVerifier(primaryBucket, bagReader, listing, resolvable, fixityListChecker, srcReader)
  }
}