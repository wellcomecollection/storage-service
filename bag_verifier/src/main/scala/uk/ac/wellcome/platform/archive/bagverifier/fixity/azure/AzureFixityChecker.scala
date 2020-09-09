package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  ExpectedFileFixity,
  FixityChecker
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureLocatable
import uk.ac.wellcome.platform.archive.common.storage.services.azure.{
  AzureLargeStreamReader,
  AzureSizeFinder
}
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class AzureFixityChecker(dynamoConfig: DynamoConfig)(implicit blobClient: BlobServiceClient, dynamoClient: AmazonDynamoDB)
    extends FixityChecker[AzureBlobLocation, AzureBlobLocationPrefix] {

  // Working out the correct value for this took a bit of experimentation; initially
  // I tried 200MB at a time, but that hit timeout errors, e.g.
  //
  //      java.util.concurrent.TimeoutException: Did not observe any item or terminal
  //      signal within 60000ms in 'map' (and no fallback has been configured))
  //
  // I had the idea to shrink the size of the buffer by looking at AzCopy, a utility
  // for downloading large blobs from Azure… as files.  The actual download code seems
  // to be in the Azure Blob Storage library for Go.  It downloads blobs 4MB at a time.
  // See https://github.com/Azure/azure-storage-blob-go/blob/48358e1de5110852097ebbc11c53581d64d47300/azblob/highlevel.go#L157-L175
  //
  // I went a bit bigger to reduce the number of requests we have to make, but
  // not by much.
  //
  // This bufferSize has successfully verified a blob which was 166 GiB in size.
  override protected val streamReader
    : Readable[AzureBlobLocation, InputStreamWithLength] =
    new AzureLargeStreamReader(bufferSize = 16 * 1000 * 1000) // 16 MB

  override protected val sizeFinder =
    new AzureSizeFinder()

  override val tags = new AzureDynamoTags(dynamoConfig)

  // e.g. ContentMD5, ContentSHA256
  // We can't include a hyphen in the name because Azure metadata names have to be
  // valid C# identifiers.
  // See https://docs.microsoft.com/en-us/rest/api/storageservices/setting-and-retrieving-properties-and-metadata-for-blob-resources#Subheading1
  override protected def fixityTagName(
    expectedFileFixity: ExpectedFileFixity
  ): String =
    s"Content${expectedFileFixity.checksum.algorithm.pathRepr.toUpperCase}"

  override implicit val locator: AzureLocatable = new AzureLocatable
}
