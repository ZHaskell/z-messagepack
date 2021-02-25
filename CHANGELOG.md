# Revision history for Z-Redis

## 0.3.0.1 -- 2021-02-25

* Add `callStream` and `StreamHandler`.

## 0.3.0.0 -- 2021-02-25

* Add `Z.IO.RPC.MessagePack` module.
* Move some instances to `Z.Data.MessagePack` to make package buildable under constrained memory.
* Remove `decodeChunks'`.
* Change `Data.Version.Version`'s instance to use array.
