package uk.ac.wellcome.platform.archive.common.storage.services

import java.io.{ByteArrayInputStream, SequenceInputStream}

import uk.ac.wellcome.platform.archive.common.storage.models.ByteRange
import uk.ac.wellcome.storage.{Identified, ReadError}
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

import scala.collection.JavaConverters._

/** If you hold open an InputStream for a long time, eventually the network times out
  * and you get an error.
  *
  * For example, from S3:
  *
  *     com.amazonaws.SdkClientException: Data read has a different length than the expected:
  *     dataLength=1234; expectedLength=56789
  *
  * and Azure:
  *
  *
  *     Could not create checksum: java.util.concurrent.TimeoutException: Did not observe
  *     any item or terminal signal within 60000ms in 'map' (and no fallback has been configured)
  *
  * To get around this issue, we read large streams in "chunks", making a separate
  * request for each segment.  The individual streams are then stitched back together
  * in a SequenceInputStream, so to the caller it's presented as a single continuous stream.
  *
  * We're hoping that making regular Get requests will keep the network "fresh", and
  * reduce the risk of a timeout while we're reading the stream.
  *
  */
trait LargeStreamReader[Ident] extends Readable[Ident, InputStreamWithLength] {
  val bufferSize: Long

  private val retries: Int = 3

  protected val sizeFinder: SizeFinder[Ident]

  protected val rangedReader: RangedReader[Ident]

  def get(ident: Ident): this.ReadEither =
    getStream(ident: Ident)

  protected def getStream(ident: Ident): this.ReadEither =
    for {
      size <- sizeFinder.getSize(ident)

      ranges = ByteRangeUtil.partition(size, bufferSize = bufferSize)

      individualStreams = ranges
        .iterator
        .map { range => getBytes(ident, range = range) }
        .map { new ByteArrayInputStream(_) }
        .asJavaEnumeration

      combinedStream = new SequenceInputStream(individualStreams)

      result = Identified(ident, new InputStreamWithLength(combinedStream, size))
    } yield result

  private def getBytes(ident: Ident, range: ByteRange): Array[Byte] = {
    // If it takes 100 GetBytes calls to read an object, to read the whole object
    // you need all 100 calls to succeed.  This becomes less likely the larger
    // the original object.
    //
    // We add some retrying logic around each of these individual calls, to increase
    // the likelihood that the overall operation will succeed.
    import uk.ac.wellcome.storage.RetryOps._

    def inner: ((Ident, ByteRange)) => Either[ReadError, Array[Byte]] =
      (args: (Ident, ByteRange)) => {
        val (ident, range) = args
        rangedReader.getBytes(ident, range = range)
      }

    inner.retry(maxAttempts = retries)((ident, range)) match {
      case Right(bytes) => bytes
      case Left(err)    => throw new RuntimeException(s"Unable to read range $range from $ident")
    }
  }
}
