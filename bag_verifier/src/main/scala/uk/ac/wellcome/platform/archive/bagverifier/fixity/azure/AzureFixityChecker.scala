package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import java.net.URI

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  ExpectedFileFixity,
  FixityChecker
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.Locatable
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureLocatable
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.services.SizeFinder
import uk.ac.wellcome.storage.services.azure.{
  AzureLargeStreamReader,
  AzureSizeFinder
}
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage.tags.Tags

class AzureFixityChecker(
  val streamReader: Readable[AzureBlobLocation, InputStreamWithLength],
  val sizeFinder: SizeFinder[AzureBlobLocation],
  val tags: Tags[AzureBlobLocation],
  val locator: Locatable[AzureBlobLocation, AzureBlobLocationPrefix, URI]
) extends FixityChecker[AzureBlobLocation, AzureBlobLocationPrefix] {

  // e.g. ContentMD5, ContentSHA256
  // We can't include a hyphen in the name because Azure metadata names have to be
  // valid C# identifiers.
  // See https://docs.microsoft.com/en-us/rest/api/storageservices/setting-and-retrieving-properties-and-metadata-for-blob-resources#Subheading1
  override protected def fixityTagName(
    expectedFileFixity: ExpectedFileFixity
  ): String =
    s"Content${expectedFileFixity.checksum.algorithm.pathRepr.toUpperCase}"
}

object AzureFixityChecker {
  def apply(
    dynamoConfig: DynamoConfig
  )(implicit blobClient: BlobServiceClient, dynamoClient: AmazonDynamoDB) = {

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
    val streamReader = AzureLargeStreamReader(bufferSize = 16 * 1024 * 1024) // 16 MB

    val sizeFinder = new AzureSizeFinder()

    val tags = new AzureDynamoTags(dynamoConfig)
    val locator = new AzureLocatable
    new AzureFixityChecker(streamReader, sizeFinder, tags, locator)
  }
}
