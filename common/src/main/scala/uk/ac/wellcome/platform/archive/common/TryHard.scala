package uk.ac.wellcome.platform.archive.common

import scala.util.{Failure, Try}

object TryHard {
  implicit class Recoverable[T](tryT: Try[T]) {
    def recoverWithMessage(message: String): Try[T] = {
      tryT.recoverWith {
        case e => Failure(new RuntimeException(s"$message: ${e.getMessage}"))
      }
    }
  }

  implicit class Available[T](maybeT: Option[T]) {
    def unavailableWithMessage(message: String): Try[T] =
      Try(maybeT.get).recoverWith {
        case e => Failure(new RuntimeException(message))
      }

  }
}
