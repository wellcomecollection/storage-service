package weco.storage_service.bag_verifier.fixity

import weco.storage_service.checksum.{ChecksumAlgorithm, ChecksumValue}

trait FixityTagChecker {

  // e.g. Content-MD5, Content-SHA256
  protected def fixityTagName(algorithm: ChecksumAlgorithm): String =
    s"Content-${algorithm.pathRepr.toUpperCase}"

  protected def fixityTagValue(value: ChecksumValue): String =
    value.toString

  case class MismatchedTag(
    name: String,
    expectedValue: String,
    actualValue: String
  ) {
    def message: String =
      s"tag $name: expected $expectedValue, saw $actualValue"
  }

  implicit class ExpectedFileFixityOps(e: ExpectedFileFixity) {
    def fixityTags: Map[String, String] =
      e.multiChecksum.definedChecksums.map {
        case (algorithm, value) =>
          fixityTagName(algorithm) -> fixityTagValue(value)
      }.toMap

    def matchesAllExistingTags(existingTags: Map[String, String]): Boolean =
      fixityTags.toSet.subsetOf(existingTags.toSet)

    def conflictsWithExistingTags(existingTags: Map[String, String]): Boolean =
      e.mismatchesWith(existingTags).nonEmpty

    def mismatchesWith(existingTags: Map[String, String]): Seq[MismatchedTag] =
      fixityTags
        .map {
          case (name, expectedValue) =>
            (name, expectedValue, existingTags.get(name))
        }
        .collect {
          case (name, expectedValue, Some(actualValue))
              if expectedValue != actualValue =>
            MismatchedTag(name, expectedValue, actualValue)
        }
        .toSeq
  }

  implicit class MapOps[K, V](m: Map[K, V]) {
    def isCompatibleWith(other: Map[K, V]): Boolean =
      m.keySet
        .intersect(other.keySet)
        .map { key =>
          (m(key), other(key))
        }
        .collect {
          case (mValue, otherValue) if mValue != otherValue =>
            (mValue, otherValue)
        }
        .isEmpty
  }
}
