package uk.ac.wellcome.platform.archive.common.verify

import uk.ac.wellcome.storage.ObjectLocation

trait Verifiable[T] {
  def create(root: ObjectLocation)(algorithm: ChecksumAlgorithm)(t: T): List[VerifiableLocation]
}

object Verifiable {
  implicit def verifiable[T](
    implicit
      verifiable: ObjectLocation => ChecksumAlgorithm => T => List[VerifiableLocation]
      ) =
    new Verifiable[T] {
      override def create(root: ObjectLocation)(algorithm: ChecksumAlgorithm)(t: T): List[VerifiableLocation] =
        verifiable(root)(algorithm)(t)
    }
}