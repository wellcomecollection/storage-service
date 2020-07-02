package uk.ac.wellcome.platform.archive.bagverifier.fixity.memory

import java.net.URI

import org.scalatest.EitherValues
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{FixityChecker, FixityCheckerTestCases}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.tags.memory.MemoryTags

class MemoryFixityCheckerTest
    extends FixityCheckerTestCases[
      MemoryLocation,
      String,
      (MemoryStreamStore[MemoryLocation], MemoryTags[MemoryLocation]),
      MemoryStreamStore[MemoryLocation]
    ]
    with EitherValues {
  type MemoryContext =
    (MemoryStreamStore[MemoryLocation], MemoryTags[MemoryLocation])

  def createMemoryTags: MemoryTags[MemoryLocation] =
    new MemoryTags[MemoryLocation](initialTags = Map.empty) {
      override def get(
        location: MemoryLocation
      ): Either[ReadError, Identified[MemoryLocation, Map[String, String]]] =
        super.get(location) match {
          case Right(tags) => Right(tags)
          case Left(_: DoesNotExistError) =>
            Right(Identified(location, Map[String, String]()))
          case Left(err) => Left(err)
        }
    }

  override def withContext[R](
    testWith: TestWith[MemoryContext, R]
  ): R =
    testWith(
      (
        MemoryStreamStore[MemoryLocation](),
        createMemoryTags
      )
    )

  override def putString(location: MemoryLocation, contents: String)(
    implicit context: MemoryContext
  ): Unit = {
    val (streamStore, _) = context
    val inputStream = stringCodec.toStream(contents).right.value
    streamStore.put(location)(inputStream) shouldBe a[Right[_, _]]
  }

  override def withStreamStore[R](
    testWith: TestWith[MemoryStreamStore[MemoryLocation], R]
  )(implicit context: MemoryContext): R = {
    val (streamStore, _) = context
    testWith(streamStore)
  }

  override def withFixityChecker[R](
    streamStore: MemoryStreamStore[MemoryLocation]
  )(
    testWith: TestWith[FixityChecker[MemoryLocation], R]
  )(implicit context: MemoryContext): R = {
    val (_, tags) = context
    testWith(
      new MemoryFixityChecker(streamStore, tags)
    )
  }

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  override def createId(implicit namespace: String): MemoryLocation =
    MemoryLocation(
      namespace = namespace,
      path = randomAlphanumeric
    )

  override def resolve(location: MemoryLocation): URI =
    new URI(location.toString)
}
