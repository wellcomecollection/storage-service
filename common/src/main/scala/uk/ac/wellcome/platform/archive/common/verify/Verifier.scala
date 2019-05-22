package uk.ac.wellcome.platform.archive.common.verify

trait Verifier {
  def verify(location: VerifiableLocation): VerifiedLocation
}
