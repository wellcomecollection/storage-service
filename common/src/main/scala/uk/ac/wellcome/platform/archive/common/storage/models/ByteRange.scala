package uk.ac.wellcome.platform.archive.common.storage.models

sealed trait ByteRange

case class OpenByteRange(start: Long) extends ByteRange
case class ClosedByteRange(start: Long, count: Long) extends ByteRange
