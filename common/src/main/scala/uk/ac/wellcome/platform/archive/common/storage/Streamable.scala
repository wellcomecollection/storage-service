package uk.ac.wellcome.platform.archive.common.storage

import java.io.InputStream


trait Streamable[T, IS <: InputStream] {
  def stream(t: T): Either[StreamUnavailable,Option[IS]]
}

object Streamable {
  implicit class StreamableOps[T, IS <: InputStream](t: T)(
    implicit streamable: Streamable[T, IS]
  ) {
    def toInputStream: Either[StreamUnavailable,Option[IS]] =
      streamable.stream(t)
  }
}


case class StreamUnavailable(msg: String, e: Option[Throwable] = None) extends Throwable