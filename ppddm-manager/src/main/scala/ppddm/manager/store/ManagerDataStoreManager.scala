package ppddm.manager.store

import java.util.UUID

import ppddm.core.store.DataStoreManager

object ManagerDataStoreManager extends DataStoreManager {
  final private val BASE_MANAGER_DIR: String = BASE_STORE_DIR + "manager"
  final private val TMP_STORE_DIR: String = BASE_MANAGER_DIR + "/tmp/"


  /**
   * Generates and returns a unique path under the directory of temporary files.
   *
   * @return
   */
  def getTmpPath(): String = {
    TMP_STORE_DIR + UUID.randomUUID().toString
  }
}
