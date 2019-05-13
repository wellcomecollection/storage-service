package uk.ac.wellcome.platform.archive.common.verify

import uk.ac.wellcome.storage.ObjectLocation


trait Verification[T] {
  def verify(root: ObjectLocation)(algorithm: ChecksumAlgorithm)(t: T): VerificationResult
}

object Verification {

  implicit def verification[Container](
                                        implicit
                                          verifiable: Verifiable[Container],
                                          verifier: Verifier
                                      ) =
    new Verification[Container] {
      override def verify(root: ObjectLocation)(algorithm: ChecksumAlgorithm)(container: Container): VerificationResult = {
        verifiable
          .create(root)(algorithm)(container)
          .map(verifier.verify)
          .foldLeft[VerificationResult](VerificationSuccess(Nil)) {

          case (VerificationSuccess(sl), s@VerifiedSuccess(_)) =>
            VerificationSuccess(s :: sl)

          case (VerificationSuccess(sl), f@VerifiedFailure(_, _)) =>
            VerificationFailure(List(f), sl)

          case (VerificationFailure(fl, sl), s@VerifiedSuccess(_)) =>
            VerificationFailure(fl, s :: sl)

          case (VerificationFailure(fl, sl), f@VerifiedFailure(_, _)) =>
            VerificationFailure(f :: fl, sl)

        }
      }
    }

  implicit class Verify[Container](container: Container)(
    implicit verification: Verification[Container]
  ) {
    def verify(root: ObjectLocation)(algorithm: ChecksumAlgorithm): VerificationResult =
      verification.verify(root)(algorithm: ChecksumAlgorithm)(container)
  }
}