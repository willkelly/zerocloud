# Description #

This middleware integrates zerovm with swift, allowing arbitrary code
to be run safely and securely on swift objects.

Code can be included as part of the execute requests or uploaded as
swift objects.  Please see the docs/ directory for additional information.

## Statsd Information ##

The following new statsd metrics will be generated.

### obj-query. ###

- `zap_transfer_time` = the amount of time it took to transfer the zap
  to the object-server node which houses the object used as input.

- `zap_failed_execution` = the number of zaps that returned a non-zero,
  non-one exit code.

- `zap_server_time` = the real time that passed on the server when
  executing the zap

- `zap_system_time` = cpu system time used by the zap execution

- `zap_user_time` = cpu user time used by the zap execution

- `zap_memory_used` = memory used by the zap execution

- `zap_swap_used` = swap used by the zap execution

- `zap_reads_from_disk` = the number of disk reads made by the zap execution

- `zap_bytes_read_from_disk` = the number of bytes read from the disk
  in zap execution

- `zap_writes_to_disk` = the number of disk writes made by the zap execution

- `zap_bytes_written_to_disk` = the number of bytes written to disk in
  zap execution

- `zap_reads_from_network` = the number of network reads made by the
  zap execution

- `zap_bytes_read_from_network` = the number of bytes read from
  network in the zap execution

- `zap_writes_to_network` = the number of writes made to the network
  in the zap execution

- `zap_bytes_written_to_network` = the number of bytes written to the
  network in the zap execution
