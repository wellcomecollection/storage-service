package uk.ac.wellcome.platform.archive.common.bagit.services

import java.io.InputStream

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagFetch,
  BagInfo,
  BagManifest,
  BagPath
}
import uk.ac.wellcome.platform.archive.common.verify.{HashingAlgorithm, SHA256}
import uk.ac.wellcome.storage.{DoesNotExistError, ObjectLocation}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

import scala.util.{Failure, Success, Try}

trait BagReader[IS <: InputStreamWithLength] {
  protected val bagFetch = BagPath("fetch.txt")
  protected val bagInfo = BagPath("bag-info.txt")
  protected val fileManifest =
    (a: HashingAlgorithm) => BagPath(s"manifest-${a.pathRepr}.txt")
  protected val tagManifest =
    (a: HashingAlgorithm) => BagPath(s"tagmanifest-${a.pathRepr}.txt")

  implicit val streamStore: StreamStore[ObjectLocation, IS]

  type Stream[T] = InputStream => Try[T]

  def get(root: ObjectLocation): Either[BagUnavailable, Bag] =
    for {
      bagInfo <- loadRequired[BagInfo](root)(bagInfo)(BagInfo.create)

      fileManifest <- loadRequired[BagManifest](root)(fileManifest(SHA256))(
        BagManifest.create(_, SHA256)
      )

      tagManifest <- loadRequired[BagManifest](root)(tagManifest(SHA256))(
        BagManifest.create(_, SHA256)
      )

      bagFetch <- loadOptional[BagFetch](root)(bagFetch)(BagFetch.create)

    } yield Bag(bagInfo, fileManifest, tagManifest, bagFetch)

  private def loadOptional[T](
    root: ObjectLocation
  )(path: BagPath)(f: Stream[T]): Either[BagUnavailable, Option[T]] = {
    val location = root.join(path.value)

    streamStore.get(location) match {
      case Right(stream) =>
        f(stream.identifiedT) match {
          case Success(r) => Right(Some(r))
          case Failure(e) =>
            Left(
              BagUnavailable(s"Error loading ${path.value}: ${e.getMessage}")
            )
        }

      case Left(_: DoesNotExistError) =>
        Right(None)

      case Left(err) =>
        Left(BagUnavailable(s"Error loading ${path.value}: $err"))
    }
  }

  private def loadRequired[T](
    root: ObjectLocation
  )(path: BagPath)(f: Stream[T]): Either[BagUnavailable, T] =
    loadOptional[T](root)(path)(f) match {
      case Right(Some(result)) => Right(result)
      case Right(None) =>
        Left(BagUnavailable(s"Error loading ${path.value}: no such file!"))
      case Left(err) => Left(err)
    }
}
