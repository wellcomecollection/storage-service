package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import org.scalatest.{Assertion, EitherValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import uk.ac.wellcome.storage.{Identified, ReadError, StoreReadError}
import weco.storage.generators.MemoryLocationGenerators
import weco.storage.providers.memory.{
  MemoryLocation,
  MemoryLocationPrefix
}
import weco.storage.store.Readable
import weco.storage.store.memory.{MemoryStore, MemoryStreamStore}
import weco.storage.streaming.Codec._
import weco.storage.streaming.InputStreamWithLength

class VerifyBagDeclarationTest
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with MemoryLocationGenerators {
  it("verifies a valid bagit.txt (v0.97)") {
    implicit val root: MemoryLocationPrefix = createMemoryLocationPrefix

    withVerifier("BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8") {
      _.verifyBagDeclaration(root) shouldBe Right(())
    }
  }

  it("verifies a valid bagit.txt (v1.0)") {
    implicit val root: MemoryLocationPrefix = createMemoryLocationPrefix

    withVerifier("BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8") {
      _.verifyBagDeclaration(root) shouldBe Right(())
    }
  }

  it("verifies a valid bagit.txt (trailing newline)") {
    implicit val root: MemoryLocationPrefix = createMemoryLocationPrefix

    withVerifier("BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n") {
      _.verifyBagDeclaration(root) shouldBe Right(())
    }
  }

  it("fails if it can't find a bagit.txt") {
    val store = MemoryStreamStore[MemoryLocation]()

    val verifier =
      new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
        override protected val streamReader
          : Readable[MemoryLocation, InputStreamWithLength] = store
      }

    val err =
      verifier.verifyBagDeclaration(createMemoryLocationPrefix).left.value
    err.userMessage.get shouldBe "Error loading Bag Declaration (bagit.txt): no such file!"
  }

  it("fails if it can't read the bagit.txt") {
    val expectedErr = new Throwable("BOOM!")

    val brokenStore = new MemoryStreamStore[MemoryLocation](
      new MemoryStore[MemoryLocation, Array[Byte]](initialEntries = Map.empty) {
        override def get(
          id: MemoryLocation
        ): Either[ReadError, Identified[MemoryLocation, Array[Byte]]] =
          Left(StoreReadError(expectedErr))
      }
    )

    val verifier =
      new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
        override protected val streamReader
          : Readable[MemoryLocation, InputStreamWithLength] = brokenStore
      }

    val err =
      verifier.verifyBagDeclaration(createMemoryLocationPrefix).left.value
    err.userMessage.get shouldBe "Error loading Bag Declaration (bagit.txt)"
    err.e shouldBe expectedErr
  }

  it("fails if the bagit.txt has no BagIt-Version line") {
    assertFails("Tag-File-Character-Encoding: UTF-8\n")
  }

  it("fails if the bagit.txt has no Tag-File-Character-Encoding line") {
    assertFails("BagIt-Version: 0.97\n")
  }

  it("fails if the bagit.txt has the wrong Tag-File-Character-Encoding") {
    assertFails("BagIt-Version: 0.97\nTag-File-Character-Encoding: Latin-1")
  }

  it("fails if the bagit.txt is empty") {
    assertFails("")
  }

  it("fails if the bagit.txt is nonsense") {
    assertFails(
      randomAlphanumeric(length = 2000),
      expectedMessage = "Error loading Bag Declaration (bagit.txt): too large"
    )
  }

  it("fails if the bagit.txt has an unwanted key") {
    assertFails(
      "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\nExtra-Key: ShouldNotBeHere"
    )
  }

  it("fails if the bagit.txt has unwanted keys") {
    assertFails(
      "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\nExtra-Key1: ShouldNotBeHere\nExtra-Key2: ShouldNotBeHere"
    )
  }

  it("fails if the bagit.txt has duplicate keys") {
    assertFails(
      "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\nBagIt-Version: 1.0"
    )
  }

  def withVerifier[R](contents: String)(
    testWith: TestWith[
      VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix],
      R
    ]
  )(implicit root: MemoryLocationPrefix): R = {
    val store = MemoryStreamStore[MemoryLocation]()

    store.put(root.asLocation("bagit.txt"))(
      stringCodec.toStream(contents).value
    )

    val verifier =
      new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
        override protected val streamReader
          : Readable[MemoryLocation, InputStreamWithLength] = store
      }

    testWith(verifier)
  }

  def assertFails[R](
    contents: String,
    expectedMessage: String =
      "Error loading Bag Declaration (bagit.txt): not correctly formatted"
  ): Assertion = {
    implicit val root: MemoryLocationPrefix = createMemoryLocationPrefix

    withVerifier(contents) { verifier =>
      val err = verifier.verifyBagDeclaration(root).left.value
      err.userMessage shouldBe Some(expectedMessage)
    }
  }
}
