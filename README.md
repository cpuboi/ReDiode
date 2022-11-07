# Rediode #
Rediode or Redis Diode replicates a redis queue over a data diode.  
It uses Reed Solomon error encoding and re-sending of the data to prevent dataloss.  


## Installation ##
First edit the two .conf files to configure the correct redis settings and the name of the queue to replicate.  
#### SystemD Installation ####
*Server on the inside of the data diode:*  
```./diode_receiver_systemd_installer.sh  ```  
*Server on the outside of the data diode:*
``` ./diode_sender_systemd_installer.sh```  


## Sender ##
The sender fetches items from a Redis queue, adds metadata and error encoding, then sends them through the diode.  

## Receiver ##
This programs listens to a UDP port, receives data, validates it and put's it on the redis queue.


### Tricks ###
To stream several redis queues, start one sender and receiver per port:queue_name  

