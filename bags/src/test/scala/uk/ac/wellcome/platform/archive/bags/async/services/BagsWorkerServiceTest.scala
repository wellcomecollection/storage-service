package uk.ac.wellcome.platform.archive.bags.async.services

import org.scalatest.{FunSpec, Matchers}

//trait WorkerServiceFixture extends SQS {
//  def withWorkerService[R](queue: Queue)(testWith: TestWith[BagsWorkerService, R]): R =
//    withSQSStream[NotificationMessage, R](queue) { sqsStream =>
//      val service = new BagsWorkerService(
//        sqsStream = sqsStream
//      )
//
//      service.run()
//
//      testWith(service)
//    }
//}

class BagsWorkerServiceTest extends FunSpec with Matchers {
  it("obeys truth") {
    true shouldBe true
  }
}
