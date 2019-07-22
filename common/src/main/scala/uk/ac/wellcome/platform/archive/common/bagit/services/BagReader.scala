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

import cats.implicits._

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

  def get(root: ObjectLocation): Either[BagReadError, Bag] =
    for {
      bagInfo <- loadRequired[BagInfo](root)(bagInfo)(BagInfo.create) leftMap BagInfoReadError

      fileManifest <- loadRequired[BagManifest](root)(fileManifest(SHA256))(
        BagManifest.create(_, SHA256)) leftMap BagManifestReadError

      tagManifest <- loadRequired[BagManifest](root)(tagManifest(SHA256))(
        BagManifest.create(_, SHA256)) leftMap TagManifestReadError

      bagFetch <- loadOptional[BagFetch](root)(bagFetch)(BagFetch.create) leftMap BagFetchReadError

    } yield Bag(bagInfo, fileManifest, tagManifest, bagFetch)

  private def loadOptional[T](root: ObjectLocation)(path: BagPath)(
    f: Stream[T]): Either[Error, Option[T]] = {
    val location = root.join(path.value)

    streamStore.get(location) match {
      case Right(stream) =>
        f(stream.identifiedT) match {
          case Success(r) => Right(Some(r))
          case Failure(e) =>
            Left(new Error(s"Error loading ${path.value}: ${e.getMessage}"))
        }

      case Left(_: DoesNotExistError) =>
        Right(None)

      case Left(err) =>
        Left(new Error(s"Error loading ${path.value}: $err"))
    }
  }

  private def loadRequired[T](root: ObjectLocation)(path: BagPath)(
    f: Stream[T]): Either[Error, T] =
    loadOptional[T](root)(path)(f) match {
      case Right(Some(result)) => Right(result)
      case Right(None) =>
        Left(new Error(s"Error loading ${path.value}: no such file!"))
      case Left(err) => Left(err)
    }
}
