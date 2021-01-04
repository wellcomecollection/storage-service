package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.storage.generators.MemoryLocationGenerators
import uk.ac.wellcome.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
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
