# Introduction to Distributed Systems

>A distributed system is one in which *components located at networked computers communicate* and *coordinate their actions only by passing messages*.D

Characteristics:

* Concurrency of Components
* Lack of a Global Clock
* Independent Failures of Components.

Resources

>It ￼￼￼extends from hardware components such as disks and printers to software-defined entities such as files, databases and data objects of all kinds. It includes the stream of video frames that emerges from a digital video camera and the audio connection that a mobile phone call represents.

## Communication Latency

Latency – *“wire delay”*
* Time to send and recv one byte of data o Depends on "distance"

Bandwidth
* Bytes/seconds
* Depends on size of vehicle

Latency is the bottleneck
* It improves slower than bandwidth : Speed of light, Routers in the middle (traffic stops)
* Request-respond cycles dominate application

## Challenges

* Secure communication over public networks
* Fault-tolerance
* Replication, caching, naming
* Coordination and shared state
