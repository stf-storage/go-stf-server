package stf

const (
  // These are currently in need of reconsideration.
  STORAGE_MODE_CRASH_RECOVERED    = -4
  STORAGE_MODE_CRASH_RECOVER_NOW  = -3
  STORAGE_MODE_CRASH              = -2

  /* Denotes that the storage is temporarily down. Use it to stop
   * the dispatcher from accessing a storage for a short period
   * of time while you do minor maintenance work. No GET/PUT/DELETE
   * will be issued against this node while in this mode.
   *
   * Upon repair worker hitting this node, the entity is deemed
   * alive, and no new entities are created to replace it.
   * This is why you should only use this mode TEMPORARILY.
   */
  STORAGE_MODE_TEMPORARILY_DOWN   = -1

  /* Denotes that the storage is for read-only. No PUT/DELETE operations
   * will be issued against this node while in this mode.
   *
   * An entity residing on this node is deemed alive.
   */
  STORAGE_MODE_READ_ONLY          = 0

  /* Denotes that the storage is for read-write. 
   * This is the default, and the most "normal" mode for a storage.
   */
  STORAGE_MODE_READ_WRITE         = 1

  /* Denotes that the storage has been retired. Marking a storage as
   * retired means that the storage is not to be put back again.
   *
   * Entities residing on this node are deemed dead. Upon repair,
   * the worker(s) will try to replace the missing entity with
   */
  STORAGE_MODE_RETIRE             = 2

  /* These are only used to denote that an automatic migration
   * is happening (XXX Unused? Legacy from a long time ago...)
   */
  STORAGE_MODE_MIGRATE_NOW        = 3
  STORAGE_MODE_MIGRATED           = 4

  /* These storages are not crashed, they don't need to be
   * emptied out, they just need to be checked for repairments
   */
  STORAGE_MODE_REPAIR             = 5
  STORAGE_MODE_REPAIR_NOW         = 6
  STORAGE_MODE_REPAIR_DONE        = 7

  /* Denotes that the storage is a spare for the registered cluster.
   * Writes are performed, but reads do not happen. Upon a failure
   * you can either replace the broken storage with this one, or
   * use this to restore the broken storage.
   */
  STORAGE_MODE_SPARE              = 10

  STORAGE_CLUSTER_MODE_READ_ONLY  = 0

  STORAGE_CLUSTER_MODE_READ_WRITE = 1
  STORAGE_CLUSTER_MODE_RETIRE     = 2
)

var WRITABLE_MODES = []int {
  STORAGE_MODE_READ_WRITE,
  STORAGE_MODE_SPARE,
}

var WRITABLE_MODES_ON_REPAIR = []int {
  STORAGE_MODE_READ_WRITE,
  STORAGE_MODE_SPARE,
  STORAGE_MODE_REPAIR,
  STORAGE_MODE_REPAIR_NOW,
  STORAGE_MODE_REPAIR_DONE,
}

