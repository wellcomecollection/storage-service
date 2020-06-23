package uk.ac.wellcome.storage.store

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.{Identified, ReadError, RetryOps}

import scala.util.{Failure, Success, Try}

trait NewRetryableReadable[Ident, T] extends Readable[Ident, T] with Logging {
  import RetryOps._

  val maxRetries: Int

  def retryableGetFunction(id: Ident): T

  def buildGetError(throwable: Throwable): ReadError

  def get(id: Ident): ReadEither =
    retryableGet(id) map { t =>
      Identified(id, t)
    }

  def retryableGet(id: Ident): Either[ReadError, T] =
    getOnce.retry(maxRetries)(id)

  private def getOnce: Ident => Either[ReadError, T] =
    (id: Ident) =>
      Try {
        retryableGetFunction(id)
      } match {
        case Success(t) => Right(t)
        case Failure(err) =>
          val error = buildGetError(err)
          warn(s"Error when trying to get $id: $error")
          Left(error)
      }
}
