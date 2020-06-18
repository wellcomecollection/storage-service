package uk.ac.wellcome.platform.archive.bagverifier.fixity

import java.io.FilterInputStream
import java.net.URI

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.fixity.memory.MemoryFixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.generators.FixityGenerators
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.storage.{
  LocateFailure,
  LocationParsingError
}
import uk.ac.wellcome.platform.archive.common.verify.{
  Checksum,
  ChecksumValue,
  MD5
}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryStreamStore}
import uk.ac.wellcome.storage.streaming.Codec.stringCodec
import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLength}
import uk.ac.wellcome.storage.tags.memory.MemoryTags

class FixityCheckerTests
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with FixityGenerators {
  override def resolve(location: ObjectLocation): URI =
    new URI(s"mem://${location.namespace}/${location.path}")

  describe("handles errors correctly") {
    it("turns an error in locate() into a FileFixityCouldNotRead") {
      val streamStore = MemoryStreamStore[ObjectLocation]()
      val tags = createMemoryTags

      val brokenChecker = new MemoryFixityChecker(streamStore, tags) {
        override def locate(
          uri: URI
        ): Either[LocateFailure[URI], ObjectLocation] =
          Left(LocationParsingError(uri, msg = "BOOM!"))
      }

      val expectedFileFixity = createExpectedFileFixity
      brokenChecker.check(expectedFileFixity) shouldBe a[FileFixityCouldNotRead]
    }

    it("handles an error when trying to checksum the object") {
      val badStream = new FilterInputStream(randomInputStream()) {
        override def read(b: Array[Byte], off: Int, len: Int): Int =
          throw new Throwable("BOOM!")
      }

      val closedStream = new InputStreamWithLength(
        badStream,
        length = randomInt(from = 1, to = 10)
      )

      closedStream.close()

      val streamStore = new MemoryStreamStore[ObjectLocation](
        memoryStore = new MemoryStore[ObjectLocation, Array[Byte]](
          initialEntries = Map.empty
        )
      ) {
        override def get(location: ObjectLocation): this.ReadEither =
          Right(Identified(location, closedStream))
      }

      val expectedFileFixity = createExpectedFileFixityWith(length = None)

      val tags = createMemoryTags

      val checker = new MemoryFixityChecker(streamStore, tags) {
        override protected val sizeFinder: SizeFinder =
          (_: ObjectLocation) => Right(closedStream.length)
      }

      checker.check(expectedFileFixity) shouldBe a[
        FileFixityCouldNotGetChecksum
      ]
    }

    it("if it can't write the fixity tags") {
      val streamStore = MemoryStreamStore[ObjectLocation]()

      val tags = new MemoryTags[ObjectLocation](initialTags = Map.empty) {
        override def get(
          location: ObjectLocation
        ): Either[ReadError, Identified[ObjectLocation, Map[String, String]]] =
          super.get(location) match {
            case Right(t) => Right(t)
            case Left(_: DoesNotExistError) =>
              Right(Identified(location, Map[String, String]()))
            case Left(err) => Left(err)
          }

        override protected def writeTags(
          id: ObjectLocation,
          tags: Map[String, String]
        ): Either[WriteError, Map[String, String]] = {
          Left(
            StoreWriteError(new Throwable("BOOM!"))
          )
        }
      }

      val contentString = "HelloWorld"
      val checksum =
        Checksum(MD5, ChecksumValue("68e109f0f40ca72a15e05cc22786f8e6"))

      val location = createObjectLocation

      val inputStream = stringCodec.toStream(contentString).right.value
      streamStore.put(location)(inputStream) shouldBe a[Right[_, _]]

      val expectedFileFixity = createExpectedFileFixityWith(
        location = location,
        checksum = checksum
      )

      val checker = new MemoryFixityChecker(streamStore, tags) {
        override protected val sizeFinder: SizeFinder =
          (_: ObjectLocation) => Right(contentString.length)
      }

      checker.check(expectedFileFixity) shouldBe a[FileFixityCouldNotWriteTag]
    }
  }

  describe("it closes the InputStream when it's done reading") {
    it("if the checksum is correct") {
      val contentHashingAlgorithm = MD5
      val contentString = "HelloWorld"
      // md5("HelloWorld")
      val contentStringChecksum =
        ChecksumValue("68e109f0f40ca72a15e05cc22786f8e6")
      val checksum = Checksum(contentHashingAlgorithm, contentStringChecksum)

      var isClosed: Boolean = false

      val inputStream: InputStreamWithLength = new InputStreamWithLength(
        Codec.stringCodec.toStream(contentString).right.get,
        length = contentString.length
      ) {
        override def close(): Unit = {
          isClosed = true
          super.close()
        }
      }

      val streamStore = new MemoryStreamStore[ObjectLocation](
        memoryStore = new MemoryStore[ObjectLocation, Array[Byte]](
          initialEntries = Map.empty
        )
      ) {
        override def get(location: ObjectLocation): this.ReadEither =
          Right(Identified(location, inputStream))
      }

      val expectedFileFixity = createExpectedFileFixityWith(checksum = checksum)

      val tags = createMemoryTags

      val checker = new MemoryFixityChecker(streamStore, tags) {
        override protected val sizeFinder: SizeFinder =
          (_: ObjectLocation) => Right(inputStream.length)
      }

      checker.check(expectedFileFixity) shouldBe a[FileFixityCorrect]

      isClosed shouldBe true
    }

    it("if the checksum is incorrect") {
      var isClosed: Boolean = false

      val inputStream: InputStreamWithLength = new InputStreamWithLength(
        randomInputStream(),
        length = randomInt(from = 1, to = 50)
      ) {
        override def close(): Unit = {
          isClosed = true
          super.close()
        }
      }

      val streamStore = new MemoryStreamStore[ObjectLocation](
        memoryStore = new MemoryStore[ObjectLocation, Array[Byte]](
          initialEntries = Map.empty
        )
      ) {
        override def get(location: ObjectLocation): this.ReadEither =
          Right(Identified(location, inputStream))
      }

      val expectedFileFixity = createExpectedFileFixity

      val tags = createMemoryTags

      val checker = new MemoryFixityChecker(streamStore, tags) {
        override protected val sizeFinder: SizeFinder =
          (_: ObjectLocation) => Right(inputStream.length)
      }

      checker.check(expectedFileFixity) shouldBe a[FileFixityMismatch]

      isClosed shouldBe true
    }
  }

  def createMemoryTags: MemoryTags[ObjectLocation] =
    new MemoryTags[ObjectLocation](initialTags = Map.empty) {
      override def get(
        location: ObjectLocation
      ): Either[ReadError, Identified[ObjectLocation, Map[String, String]]] =
        super.get(location) match {
          case Right(tags) => Right(tags)
          case Left(_: DoesNotExistError) =>
            Right(Identified(location, Map[String, String]()))
          case Left(err) => Left(err)
        }
    }
}
