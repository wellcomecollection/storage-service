package weco.storage_service.bag_verifier.fixity

import weco.storage_service.checksum.{ChecksumAlgorithm, ChecksumValue, MismatchedChecksum}

trait FixityTagChecker {

  // e.g. Content-MD5, Content-SHA256
  protected def fixityTagName(algorithm: ChecksumAlgorithm): String =
    s"Content-${algorithm.pathRepr.toUpperCase}"

  protected def fixityTagValue(value: ChecksumValue): String =
    value.toString

  implicit class ExpectedFileFixityOps(e: ExpectedFileFixity) {
    def fixityTags: Map[String, String] =
      Map(
        fixityTagName(e.checksum.algorithm) -> fixityTagValue(e.checksum.value)
      )

    def matchesAllExistingTags(existingTags: Map[String, String]): Boolean =
      fixityTags.toSet.subsetOf(existingTags.toSet)

    def conflictsWithExistingTags(existingTags: Map[String, String]): Boolean =
      e.findMismatches(existingTags).nonEmpty

    def findMismatches(existingTags: Map[String, String]): Set[MismatchedChecksum] =
      Seq((e.checksum.algorithm, e.checksum.value))
        .map { case (algorithm, expected) =>
          (algorithm, expected, existingTags.get(fixityTagName(algorithm)).map(ChecksumValue(_)))
        }
        .collect { case (algorithm, expected, Some(actual)) if expected != actual =>
          MismatchedChecksum(algorithm = algorithm, expected = expected, actual = actual)
        }
        .toSet
  }
}
