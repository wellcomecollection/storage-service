package uk.ac.wellcome.platform.archive.common.storage.services

import java.io.{ByteArrayInputStream, SequenceInputStream}

import uk.ac.wellcome.storage.Identified
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

import scala.collection.JavaConverters._

trait LargeStreamReader[Ident] extends Readable[Ident, InputStreamWithLength] {
  val bufferSize: Long

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
        .map { range => rangedReader.getBytes(ident, range = range) }
        .map { new ByteArrayInputStream(_) }
        .asJavaEnumeration

      combinedStream = new SequenceInputStream(individualStreams)

      result = Identified(ident, new InputStreamWithLength(combinedStream, size))
    } yield result
}
