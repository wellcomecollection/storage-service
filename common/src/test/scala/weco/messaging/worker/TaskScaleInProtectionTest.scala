package weco.messaging.worker

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class TaskScaleInProtectionTest extends AnyFunSpec with Matchers {

  class RecordingProtection
      extends EcsTaskScaleInProtection(
        "http://localhost",
        expiresInMinutes = 1
      ) {
    val calls: mutable.Buffer[Boolean] = mutable.Buffer.empty

    override protected def setProtection(enabled: Boolean): Unit =
      calls += enabled
  }

  it("protects on the first acquire and clears on the last release") {
    val protection = new RecordingProtection()

    protection.acquire()
    protection.acquire()
    protection.calls shouldBe Seq(true)

    protection.release()
    protection.calls shouldBe Seq(true)

    protection.release()
    protection.calls shouldBe Seq(true, false)
  }

  it("re-protects when work starts again after going idle") {
    val protection = new RecordingProtection()

    protection.acquire()
    protection.release()
    protection.acquire()

    protection.calls shouldBe Seq(true, false, true)
  }
}
