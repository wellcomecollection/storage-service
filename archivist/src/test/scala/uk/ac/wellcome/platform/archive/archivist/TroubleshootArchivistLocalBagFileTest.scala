package uk.ac.wellcome.platform.archive.archivist

import java.io.File

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.archivist.fixtures.ArchivistFixtures

// Useful test to troubleshoot running the archivist using a local bagfile
class TroubleshootArchivistLocalBagFileTest
    extends FunSpec
    with Matchers
    with ArchivistFixtures {

  ignore("downloads, uploads and verifies a known BagIt bag") {
    withArchivist() {
      case (ingestBucket, storageBucket, queuePair, _, _) =>
        sendBag(
          new File(
            List(
              System.getProperty("user.home"),
              "Desktop", "bag-ingest",
              "g9-4cbdce3c-e40a-4234-96ec-fad5f53210c0.zip"
//              "b22454408.zip"
            ).mkString("/")),
          ingestBucket,
          queuePair.queue
        ) { _ =>
          while (true) {
            Thread.sleep(1000)
            println(s"Uploaded: ${listKeysInBucket(storageBucket)}")
          }
        }
    }
  }
}


//new File(
//List(
//System.getProperty("user.home"),
//"Desktop", "bag-ingest",
//"g9-4cbdce3c-e40a-4234-96ec-fad5f53210c0.zip"
//).mkString("/")),
//ingestBucket,
//queuePair
//) { _ =>
//  while (true) {
//  Thread.sleep(10000)
//  println(s"Uploaded: ${listKeysInBucket(storageBucket)}")
//}
//}