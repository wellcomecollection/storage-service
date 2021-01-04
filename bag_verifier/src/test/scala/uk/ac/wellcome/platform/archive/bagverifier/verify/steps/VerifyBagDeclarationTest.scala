package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.storage.{Identified, ReadError, StoreReadError}
import uk.ac.wellcome.storage.generators.MemoryLocationGenerators
import uk.ac.wellcome.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryStreamStore}
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class VerifyBagDeclarationTest extends AnyFunSpec with Matchers with EitherValues with MemoryLocationGenerators {
  it("verifies a valid bagit.txt (v0.97)") {
    val store = MemoryStreamStore[MemoryLocation]()

    val root = createMemoryLocationPrefix

    store.put(root.asLocation("bagit.txt"))(
      stringCodec.toStream("BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8").value
    )

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = store
    }

    verifier.verifyBagDeclaration(root) shouldBe Right(())
  }

  it("verifies a valid bagit.txt (v1.0)") {
    val store = MemoryStreamStore[MemoryLocation]()

    val root = createMemoryLocationPrefix

    store.put(root.asLocation("bagit.txt"))(
      stringCodec.toStream("BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8").value
    )

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = store
    }

    verifier.verifyBagDeclaration(root) shouldBe Right(())
  }

  it("verifies a valid bagit.txt trailing newline)") {
    val store = MemoryStreamStore[MemoryLocation]()

    val root = createMemoryLocationPrefix

    store.put(root.asLocation("bagit.txt"))(
      stringCodec.toStream("BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n").value
    )

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = store
    }

    verifier.verifyBagDeclaration(root) shouldBe Right(())
  }

  it("fails if it can't find a bagit.txt") {
    val store = MemoryStreamStore[MemoryLocation]()

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = store
    }

    val err = verifier.verifyBagDeclaration(createMemoryLocationPrefix).left.value
    err.userMessage.get shouldBe "No Bag Declaration in bag (no bagit.txt)"
  }

  it("fails if it can't read the bagit.txt") {
    val expectedErr = new Throwable("BOOM!")

    val brokenStore = new MemoryStreamStore[MemoryLocation](
      new MemoryStore[MemoryLocation, Array[Byte]](initialEntries = Map.empty) {
        override def get(id: MemoryLocation): Either[ReadError, Identified[MemoryLocation, Array[Byte]]] =
          Left(StoreReadError(expectedErr))
      }
    )

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = brokenStore
    }

    val err = verifier.verifyBagDeclaration(createMemoryLocationPrefix).left.value
    err.userMessage.get shouldBe "Unable to read Bag Declaration from bag (bagit.txt)"
    err.e shouldBe expectedErr
  }

  it("fails if the bagit.txt has no BagIt-Version line") {
    val store = MemoryStreamStore[MemoryLocation]()

    val root = createMemoryLocationPrefix

    store.put(root.asLocation("bagit.txt"))(
      stringCodec.toStream("Tag-File-Character-Encoding: UTF-8\n").value
    )

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = store
    }

    val err = verifier.verifyBagDeclaration(createMemoryLocationPrefix).left.value
    err.userMessage.get shouldBe "The Bag Declaration is missing BagIt-Version (bagit.txt)"
  }

  it("fails if the bagit.txt has no Tag-File-Character-Encoding line") {
    val store = MemoryStreamStore[MemoryLocation]()

    val root = createMemoryLocationPrefix

    store.put(root.asLocation("bagit.txt"))(
      stringCodec.toStream("BagIt-Version: 0.97\n").value
    )

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = store
    }

    val err = verifier.verifyBagDeclaration(createMemoryLocationPrefix).left.value
    err.userMessage.get shouldBe "The Bag Declaration is missing Tag-File-Character-Encoding (bagit.txt)"
  }

  it("fails if the bagit.txt is empty") {
    val store = MemoryStreamStore[MemoryLocation]()

    val root = createMemoryLocationPrefix

    store.put(root.asLocation("bagit.txt"))(
      stringCodec.toStream("BagIt-Version: 0.97\n").value
    )

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = store
    }

    val err = verifier.verifyBagDeclaration(createMemoryLocationPrefix).left.value
    err.userMessage.get shouldBe "The Bag Declaration is missing BagIt-Version and Tag-File-Character-Encoding (bagit.txt)"
  }

  it("fails if the bagit.txt has an unwanted key") {
    val store = MemoryStreamStore[MemoryLocation]()

    val root = createMemoryLocationPrefix

    store.put(root.asLocation("bagit.txt"))(
      stringCodec.toStream("BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\nExtra-Key: ShouldNotBeHere").value
    )

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = store
    }

    val err = verifier.verifyBagDeclaration(createMemoryLocationPrefix).left.value
    err.userMessage.get shouldBe "The Bag Declaration has an unwanted key: Extra-Key (bagit.txt)"
  }

  it("fails if the bagit.txt has unwanted keys") {
    val store = MemoryStreamStore[MemoryLocation]()

    val root = createMemoryLocationPrefix

    store.put(root.asLocation("bagit.txt"))(
      stringCodec.toStream("BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\nExtra-Key1: ShouldNotBeHere\nExtra-Key2: ShouldNotBeHere").value
    )

    val verifier = new VerifyBagDeclaration[MemoryLocation, MemoryLocationPrefix] {
      override protected val srcReader: Readable[MemoryLocation, InputStreamWithLength] = store
    }

    val err = verifier.verifyBagDeclaration(createMemoryLocationPrefix).left.value
    err.userMessage.get shouldBe "The Bag Declaration has unwanted keys: Extra-Key1, Extra-Key2 (bagit.txt)"
  }
}
