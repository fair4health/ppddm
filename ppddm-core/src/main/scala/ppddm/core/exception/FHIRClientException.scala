package ppddm.core.exception

final case class FHIRClientException(private val reason: String, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}
