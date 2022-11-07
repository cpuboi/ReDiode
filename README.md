# Rediode #
Rediode or Redis Diode replicates a redis queue over a data diode.  
It uses Reed Solomon error encoding and re-sending of the data to prevent dataloss.  

## Sender ##
The sender fetches items from a Redis queue, adds metadata and error encoding, then sends them through the diode.  

## Receiver ##
This programs listens to a UDP port, receives data, validates it and put's it on the redis queue.


### Tricks ###
To stream several redis queues, start one sender and receiver per port:queue_name  

