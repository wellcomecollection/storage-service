package weco.storage.providers.azure

import com.azure.storage.blob.models.BlobStorageException
import weco.storage.{
  DoesNotExistError,
  ReadError,
  RetryableError,
  StoreReadError
}

object AzureStorageErrors {
  val readErrors: PartialFunction[Throwable, ReadError] = {
    case exc: BlobStorageException if exc.getStatusCode == 404 =>
      DoesNotExistError(exc)

    case exc if exc.getMessage.contains("TimeoutException") =>
      // Timeout errors from Azure should be retried and are of the form
      //
      //    reactor.core.Exceptions$ReactiveException: java.util.concurrent.TimeoutException:
      //    Did not observe any item or terminal signal within 60000ms in 'map'
      //    (and no fallback has been configured)
      //
      // Note: we cannot make assertions on the type ReactiveException; for
      // some reason it's a type we can't import it for isInstanceOf[…].
      new StoreReadError(exc) with RetryableError

    case exc if exc.getMessage.contains("NativeIoException") =>
      // IO errors from Azure should be retried and are of the form
      //
      //    reactor.core.Exceptions$ReactiveException: io.netty.channel.unix.Errors$NativeIoException:
      //    readAddress(..) failed: Connection reset by peer
      //
      // Note: we cannot make assertions on the type ReactiveException; for
      // some reason it's a type we can't import it for isInstanceOf[…].
      new StoreReadError(exc) with RetryableError

    case exc =>
      StoreReadError(exc)
  }
}
