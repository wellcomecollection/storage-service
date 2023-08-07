package weco.storage.store.azure

import java.io.BufferedInputStream

import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.models.{BlobErrorCode, BlobStorageException}
import weco.storage.providers.azure.AzureBlobLocation
import weco.storage.store.Writable
import weco.storage.streaming.InputStreamWithLength
import weco.storage.{Identified, OverwriteError, StoreWriteError}

import scala.util.{Failure, Success, Try}

trait AzureStreamWritable
    extends Writable[AzureBlobLocation, InputStreamWithLength] {
  implicit val blobClient: BlobServiceClient
  val allowOverwrites: Boolean

  override def put(location: AzureBlobLocation)(
    inputStream: InputStreamWithLength): WriteEither =
    Try {
      val individualBlobClient =
        blobClient
          .getBlobContainerClient(location.container)
          .getBlobClient(location.name)

      // We need to buffer the input stream, or we get errors from the Azure SDK:
      //
      //    java.io.IOException: mark/reset not supported
      //
      // The Azure SDK checks the length by counting how many bytes it's already read,
      // and how many are remaining according to `available()`.  That's an estimate
      // of how many bytes can be read without blocking; it guesses wrong and you
      // get a different error from the Azure SDK, of the form:
      //
      //    Request body emitted 20 bytes, more than the expected 19 bytes
      //
      // Taking control of this manually gets it to work.
      //
      // I added this after failures in the S3toAzureTransfer tests; I don't know how
      // to test the need for this wrapper directly.
      //
      val bufferedStream: BufferedInputStream =
        new BufferedInputStream(inputStream) {
          override def available(): Int = {
            val remaining = inputStream.length - pos

            if (remaining > Int.MaxValue) Int.MaxValue else remaining.toInt
          }
        }

      individualBlobClient.upload(
        bufferedStream,
        inputStream.length.toLong,
        allowOverwrites
      )

    } match {
      case Success(_) => Right(Identified(location, inputStream))
      case Failure(
          exc: BlobStorageException
          ) if exc.getErrorCode == BlobErrorCode.BLOB_ALREADY_EXISTS =>
        Left(OverwriteError(exc))
      case Failure(throwable) => Left(StoreWriteError(throwable))
    }
}
