package uk.ac.wellcome.platform.archive.bagverifier.fixity

import java.io.FilterInputStream
import java.net.URI

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.fixity.memory.MemoryFixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.generators.FixityGenerators
import uk.ac.wellcome.platform.archive.bagverifier.storage.{
  Locatable,
  LocateFailure,
  LocationParsingError
}
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.verify.{
  Checksum,
  ChecksumValue,
  MD5
}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.providers.memory.{
  MemoryLocation,
  MemoryLocationPrefix
}
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryStreamStore}
import uk.ac.wellcome.storage.streaming.Codec.stringCodec
import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLength}
import uk.ac.wellcome.storage.tags.memory.MemoryTags

class FixityCheckerTests
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with FixityGenerators[MemoryLocation] {
  override def resolve(location: MemoryLocation): URI =
    new URI(s"mem://$location")

  override def createLocation: MemoryLocation =
    MemoryLocation(
      namespace = randomAlphanumeric,
      path = randomAlphanumeric
    )

  describe("handles errors correctly") {
    it("turns an error in locate() into a FileFixityCouldNotRead") {
      val streamStore = MemoryStreamStore[MemoryLocation]()
      val tags = createMemoryTags

      val brokenChecker = new MemoryFixityChecker(streamStore, tags) {
        override val locator =
          new Locatable[MemoryLocation, MemoryLocationPrefix, URI] {
            def locate(
              uri: URI
            )(
              maybeRoot: Option[MemoryLocationPrefix]
            ): Either[LocateFailure[URI], MemoryLocation] =
              Left(LocationParsingError(uri, msg = "BOOM!"))
          }
      }

      val expectedFileFixity = createExpectedFileFixity
      brokenChecker.check(expectedFileFixity) shouldBe a[FileFixityCouldNotRead[
        _
      ]]
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

      val streamStore = new MemoryStreamStore[MemoryLocation](
        memoryStore = new MemoryStore[MemoryLocation, Array[Byte]](
          initialEntries = Map.empty
        )
      ) {
        override def get(location: MemoryLocation): this.ReadEither =
          Right(Identified(location, closedStream))
      }

      val expectedFileFixity = createDataDirectoryFileFixity

      val tags = createMemoryTags

      val checker = new MemoryFixityChecker(streamStore, tags) {
        override protected val sizeFinder: SizeFinder[MemoryLocation] =
          new SizeFinder[MemoryLocation] {
            override def get(location: MemoryLocation): ReadEither =
              Right(Identified(location, closedStream.length))
          }
      }

      checker.check(expectedFileFixity) shouldBe a[
        FileFixityCouldNotGetChecksum[_]
      ]
    }

    it("if it can't write the fixity tags") {
      val streamStore = MemoryStreamStore[MemoryLocation]()

      val tags = Some(new MemoryTags[MemoryLocation](initialTags = Map.empty) {
        override def get(
          location: MemoryLocation
        ): Either[ReadError, Identified[MemoryLocation, Map[String, String]]] =
          super.get(location) match {
            case Right(t) => Right(t)
            case Left(_: DoesNotExistError) =>
              Right(Identified(location, Map[String, String]()))
            case Left(err) => Left(err)
          }

        override protected def writeTags(
          id: MemoryLocation,
          tags: Map[String, String]
        ): Either[WriteError, Map[String, String]] = {
          Left(
            StoreWriteError(new Throwable("BOOM!"))
          )
        }
      })

      val contentString = "HelloWorld"
      val checksum =
        Checksum(MD5, ChecksumValue("68e109f0f40ca72a15e05cc22786f8e6"))

      val location = MemoryLocation(
        namespace = randomAlphanumeric,
        path = randomAlphanumeric
      )

      val inputStream = stringCodec.toStream(contentString).right.value
      streamStore.put(location)(inputStream) shouldBe a[Right[_, _]]

      val expectedFileFixity = createDataDirectoryFileFixityWith(
        location = location,
        checksum = checksum
      )

      val checker = new MemoryFixityChecker(streamStore, tags) {
        override protected val sizeFinder: SizeFinder[MemoryLocation] =
          new SizeFinder[MemoryLocation] {
            override def get(location: MemoryLocation): ReadEither =
              Right(Identified(location, contentString.length))
          }
      }

      checker.check(expectedFileFixity) shouldBe a[FileFixityCouldNotWriteTag[
        _
      ]]
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

      val streamStore = new MemoryStreamStore[MemoryLocation](
        memoryStore = new MemoryStore[MemoryLocation, Array[Byte]](
          initialEntries = Map.empty
        )
      ) {
        override def get(location: MemoryLocation): ReadEither =
          Right(Identified(location, inputStream))
      }

      val expectedFileFixity =
        createDataDirectoryFileFixityWith(checksum = checksum)

      val tags = createMemoryTags

      val checker = new MemoryFixityChecker(streamStore, tags) {
        override protected val sizeFinder: SizeFinder[MemoryLocation] =
          new SizeFinder[MemoryLocation] {
            override def get(location: MemoryLocation): ReadEither =
              Right(Identified(location, inputStream.length))
          }
      }

      checker.check(expectedFileFixity) shouldBe a[FileFixityCorrect[_]]

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

      val streamStore = new MemoryStreamStore[MemoryLocation](
        memoryStore = new MemoryStore[MemoryLocation, Array[Byte]](
          initialEntries = Map.empty
        )
      ) {
        override def get(location: MemoryLocation): ReadEither =
          Right(Identified(location, inputStream))
      }

      val expectedFileFixity = createExpectedFileFixity

      val tags = createMemoryTags

      val checker = new MemoryFixityChecker(streamStore, tags) {
        override protected val sizeFinder: SizeFinder[MemoryLocation] =
          new SizeFinder[MemoryLocation] {
            override def get(location: MemoryLocation): ReadEither =
              Right(Identified(location, inputStream.length))
          }
      }

      checker.check(expectedFileFixity) shouldBe a[FileFixityMismatch[_]]

      isClosed shouldBe true
    }
  }

  def createMemoryTags: Option[MemoryTags[MemoryLocation]] =
    Some(new MemoryTags[MemoryLocation](initialTags = Map.empty) {
      override def get(
        location: MemoryLocation
      ): Either[ReadError, Identified[MemoryLocation, Map[String, String]]] =
        super.get(location) match {
          case Right(tags) => Right(tags)
          case Left(_: DoesNotExistError) =>
            Right(Identified(location, Map[String, String]()))
          case Left(err) => Left(err)
        }
    })
}
