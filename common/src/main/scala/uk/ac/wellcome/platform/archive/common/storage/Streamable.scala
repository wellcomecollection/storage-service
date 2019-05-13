package uk.ac.wellcome.platform.archive.common.storage

import java.io.InputStream

import scala.util.Try


trait Streamable[T, IS <: InputStream] {
  def stream(t: T): Try[IS]
}

object Streamable {
  implicit def streamable[T, IS <: InputStream](
    implicit
      streamConverter: T => Try[IS]
  ) =
    new Streamable[T, IS] {
      override def stream(t: T): Try[IS] =
        streamConverter(t)
    }

  implicit class StreamableOps[T, IS <: InputStream](t: T)(
    implicit streamable: Streamable[T, IS]
  ) {
    def toInputStream: Try[InputStream] =
      streamable.stream(t)
  }
}