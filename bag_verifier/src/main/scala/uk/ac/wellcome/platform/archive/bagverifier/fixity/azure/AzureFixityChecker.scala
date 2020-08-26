package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity.FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureLocatable
import uk.ac.wellcome.platform.archive.common.storage.services.azure.{
  AzureLargeStreamReader,
  AzureSizeFinder
}
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class AzureFixityChecker(implicit blobClient: BlobServiceClient)
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
    new AzureLargeStreamReader(bufferSize = 16 * 1000 * 1000)  // 16 MB

  override protected val sizeFinder =
    new AzureSizeFinder()

  /**
    * The FixityChecker tags objects with their checksum so that,
    * if they are lifecycled to cold storage, we don't need to read them to know the checksum.
    * This is useful for files referenced in the fetch file. However, references in the fetch file
    * will always point to the primary bucket in S3, never Azure, so there's no need to tag in the AzureFixityChecker
    */
  override val tags = None

  override implicit val locator: AzureLocatable = new AzureLocatable
}
