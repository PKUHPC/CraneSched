## Fix Inconsistency

All RPCs are executed concurrently and some inconsistency may occur. Fix them.

- [ ] Task creation and termination may run at the same time, and it will cause undefined behavior. Possible solution:
  Add a mutex in ITask structure.