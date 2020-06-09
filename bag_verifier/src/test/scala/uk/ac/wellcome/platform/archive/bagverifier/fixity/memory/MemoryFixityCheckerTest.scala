package uk.ac.wellcome.platform.archive.bagverifier.fixity.memory

import java.net.URI

import org.scalatest.EitherValues
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  FixityChecker,
  FixityCheckerTestCases
}
import uk.ac.wellcome.storage.{
  DoesNotExistError,
  ObjectLocation,
  ReadError
}
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.tags.memory.MemoryTags

class MemoryFixityCheckerTest
    extends FixityCheckerTestCases[String, (MemoryStreamStore[ObjectLocation], MemoryTags[ObjectLocation]), MemoryStreamStore[ObjectLocation]]
    with EitherValues {
  type MemoryContext = (MemoryStreamStore[ObjectLocation], MemoryTags[ObjectLocation])

  def createMemoryTags: MemoryTags[ObjectLocation] =
    new MemoryTags[ObjectLocation](initialTags = Map.empty) {
      override def get(location: ObjectLocation): Either[ReadError, Map[String, String]] =
        super.get(location) match {
          case Right(tags)                => Right(tags)
          case Left(_: DoesNotExistError) => Right(Map[String, String]())
          case Left(err)                  => Left(err)
        }
    }

  override def withContext[R](
    testWith: TestWith[MemoryContext, R]
  ): R =
    testWith((
      MemoryStreamStore[ObjectLocation](),
      createMemoryTags
    ))

  override def putString(location: ObjectLocation, contents: String)(
    implicit context: MemoryContext
  ): Unit = {
    val (streamStore, _) = context
    val inputStream = stringCodec.toStream(contents).right.value
    streamStore.put(location)(inputStream) shouldBe a[Right[_, _]]
  }

  override def withStreamStore[R](testWith: TestWith[MemoryStreamStore[ObjectLocation], R])(implicit context: MemoryContext): R = {
    val (streamStore, _) = context
    testWith(streamStore)
  }

  override def withFixityChecker[R](streamStore: MemoryStreamStore[ObjectLocation])(testWith: TestWith[FixityChecker, R])(implicit context: MemoryContext): R = {
    val (_, tags) = context
    testWith(
      new MemoryFixityChecker(streamStore, tags)
    )
  }

  override def createObjectLocationWith(namespace: String): ObjectLocation =
    ObjectLocation(
      namespace = namespace,
      path = randomAlphanumeric
    )

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  override def createId(implicit namespace: String): ObjectLocation =
    createObjectLocationWith(namespace)

  override def resolve(location: ObjectLocation): URI =
    new URI(s"mem://${location.namespace}/${location.path}")
}
