package uk.ac.wellcome.platform.archive.common.verify

import uk.ac.wellcome.platform.archive.common.storage.StorageContainer

trait Verifiable[F[_], Container <: StorageContainer] {
  def verifiable(container: Container): F[VerifiableObjectLocation]
}

object Verifiable {
  implicit def verifiable[F[_], Container <: StorageContainer](
                                                                implicit verifier: Container => F[VerifiableObjectLocation]
                                                              ) =
    new Verifiable[F, Container] {
      override def verifiable(container: Container): F[VerifiableObjectLocation] = verifier(container)
    }

  implicit class Verifier[Container <: StorageContainer](container: Container)(implicit verifier: Verifiable[Seq, Container]) {
    def verifiable: Seq[VerifiableObjectLocation] =
      verifier.verifiable(container)
  }
}