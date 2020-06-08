package uk.ac.wellcome.platform.storage.bag_tagger

import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.messaging.memory.MemoryMessageSender
import uk.ac.wellcome.platform.storage.bag_tagger.fixtures.BagTaggerFixtures
import uk.ac.wellcome.json.JsonUtil._

class BagTaggerFeatureTest
    extends AnyFunSpec
    with BagTaggerFixtures
    with Eventually
    with Matchers {

  it("should consume messages") {
    withLocalSqsQueue() { queue =>
      val outgoing = new MemoryMessageSender()
      withWorkerService(
        queue,
        outgoing
      ) { _ =>
        eventually {
          assertQueueEmpty(queue)
        }
      }
    }
  }
}
