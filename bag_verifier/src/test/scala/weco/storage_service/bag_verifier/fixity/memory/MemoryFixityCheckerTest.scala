package weco.storage_service.bag_verifier.fixity.memory

import java.net.URI

import org.scalatest.EitherValues
import weco.fixtures.TestWith
import weco.storage_service.bag_verifier.fixity.{
  FixityChecker,
  FixityCheckerTagsTestCases
}
import weco.storage._
import weco.storage.generators.MemoryLocationGenerators
import weco.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import weco.storage.store.memory.MemoryStreamStore
import weco.storage.streaming.Codec._
import weco.storage.tags.memory.MemoryTags

class MemoryFixityCheckerTest
    extends FixityCheckerTagsTestCases[
      MemoryLocation,
      MemoryLocationPrefix,
      String,
      (MemoryStreamStore[MemoryLocation], MemoryTags[MemoryLocation]),
      MemoryStreamStore[MemoryLocation]
    ]
    with EitherValues
    with MemoryLocationGenerators {
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
    val inputStream = stringCodec.toStream(contents).value
    streamStore.put(location)(inputStream) shouldBe a[Right[_, _]]
  }

  override def withStreamReader[R](
    testWith: TestWith[MemoryStreamStore[MemoryLocation], R]
  )(implicit context: MemoryContext): R = {
    val (streamStore, _) = context
    testWith(streamStore)
  }

  override def withFixityChecker[R](
    memoryReader: MemoryStreamStore[MemoryLocation]
  )(
    testWith: TestWith[FixityChecker[MemoryLocation, MemoryLocationPrefix], R]
  )(implicit context: MemoryContext): R = {
    val (_, tags) = context
    testWith(
      new MemoryFixityChecker(memoryReader, tags)
    )
  }

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric())

  override def createId(implicit namespace: String): MemoryLocation =
    createMemoryLocation

  override def resolve(location: MemoryLocation): URI =
    new URI(s"mem://$location")
}
