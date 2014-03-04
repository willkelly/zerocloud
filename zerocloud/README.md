# Description #

This middleware integrates zerovm with swift, allowing arbitrary code
to be run safely and securely on swift objects.

Code can be included as part of the execute requests or uploaded as
swift objects.  Please see the docs/ directory for additional information.

## Statsd Information ##

The following new statsd metrics will be generated.

### obj-query. ###

- zap_transfer_time = the amount of time it took to transfer the zap
  to the object-server node which houses the object used as input.
