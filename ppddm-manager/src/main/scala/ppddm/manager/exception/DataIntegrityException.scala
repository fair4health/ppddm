package ppddm.manager.exception

final case class DataIntegrityException(private val reason: String, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
}
