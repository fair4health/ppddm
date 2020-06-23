package ppddm.core.exception

final case class NotFoundException(private val reason: String, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}
