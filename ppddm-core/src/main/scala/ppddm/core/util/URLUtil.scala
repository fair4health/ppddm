package ppddm.core.util

/**
 * Utilities for URL processing
 */
object URLUtil {

  /**
   * Appends the given path elements by joining with '/' in between.
   * Handles the leading and trailing '/' characters if exist in paths.
   *
   * @param paths
   * @return
   */
  def append(paths: String*): String = {
    if (paths.size == 1) {
      paths(0)
    } else {
      paths.map(p => {
        p.stripPrefix("/").stripSuffix("/")
      }).mkString("/")
    }
  }

}
