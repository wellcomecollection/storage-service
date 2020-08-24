package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import com.azure.storage.blob.BlobServiceClient
import uk.ac.wellcome.platform.archive.bagverifier.fixity._
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureLocatable
import uk.ac.wellcome.platform.archive.common.storage.services.azure.AzureSizeFinder
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.store.azure.AzureStreamStore
import java.util.concurrent.TimeoutException

import io.netty.channel.unix.Errors.NativeIoException

class AzureFixityChecker(implicit blobClient: BlobServiceClient)
    extends FixityChecker[AzureBlobLocation, AzureBlobLocationPrefix] {
  override protected val streamStore =
    new AzureStreamStore()

  override protected val sizeFinder =
    new AzureSizeFinder()

  import uk.ac.wellcome.storage.RetryOps._

  override def check(expectedFileFixity: ExpectedFileFixity): FileFixityResult[AzureBlobLocation] = {

    // We've seen occasional errors when verifying objects in Azure, which are of
    // the form:
    //
    //      Could not create checksum: java.util.concurrent.TimeoutException: Did not observe
    //      any item or terminal signal within 60000ms in 'map' (and no fallback has been configured)
    //
    // and:
    //
    //      WARN  r.n.http.client.HttpClientConnect - R:wecostoragestage.blob.core.windows.net/...]
    //	    io.netty.channel.unix.Errors$NativeIoException: readAddress(..) failed: Connection reset by peer
    //
    // This isn't handled by retrying logic inside the AzureStreamStore -- that only covers
    // opening a stream; if the stream errors out after it's been opened, the retrying in the
    // Store doesn't cover it.
    //
    // Retrying the entire bag usually clears up the timeout, so we allow retrying on
    // a pre-file level.
    //
    // The implementation of inner() is working around the slight oddity that RetryOps only
    // knows how to retry a function that returns an Either.
    def inner: ExpectedFileFixity => Either[FileFixityError[AzureBlobLocation], FileFixityResult[AzureBlobLocation]] =
      (expectedFileFixity: ExpectedFileFixity) =>
        super.check(expectedFileFixity) match {
          case error: FileFixityError[AzureBlobLocation] if error.e.isInstanceOf[TimeoutException] =>
            Left(error)

          case error: FileFixityError[AzureBlobLocation] if error.e.isInstanceOf[NativeIoException] =>
            Left(error)

          case result => Right(result)
        }

    inner.retry(maxAttempts = 3)(expectedFileFixity).right.get
  }

  /**
    * The FixityChecker tags objects with their checksum so that,
    * if they are lifecycled to cold storage, we don't need to read them to know the checksum.
    * This is useful for files referenced in the fetch file. However, references in the fetch file
    * will always point to the primary bucket in S3, never Azure, so there's no need to tag in the AzureFixityChecker
    */
  override val tags = None

  override implicit val locator: AzureLocatable = new AzureLocatable
}
