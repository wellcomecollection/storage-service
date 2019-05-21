package uk.ac.wellcome.platform.archive.common.storage.services

import java.io.InputStream

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.{Resolvable, Streamable}
import uk.ac.wellcome.storage.{ObjectLocation, StorageBackend}

import scala.util.Try

object StreamableInstances {

  implicit class ObjectLocationStreamable(location: ObjectLocation)(
    implicit storageBackend: StorageBackend)
      extends Logging {
    def toInputStream: Try[InputStream] = {
      debug(s"Converting $location to InputStream")

      val result = storageBackend.get(location)

      debug(s"Got: $result")

      result
    }
  }

  implicit class ResolvableStreamable[T](t: T)(implicit storageBackend: StorageBackend,
                                               resolver: Resolvable[T])
      extends Logging {
    def from(root: ObjectLocation): Try[InputStream] = {
      debug(s"Attempting to resolve Streamable $t")

      val streamable = new Streamable[T, InputStream] {
        override def stream(t: T): Try[InputStream] = {
          debug(s"Converting $t to InputStream")

          val resolvedLocation: ObjectLocation = resolver.resolve(root)(t)

          resolvedLocation.toInputStream
        }
      }

      streamable.stream(t)
    }
  }
}
