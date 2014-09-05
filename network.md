# Network

## Layering

### Physical Layer

`Deals with representing bits`

* Transmits and receives raw data to communication medium
* Does not care about contents
* Media, voltage levels, speed, connectors
* Examples: USB, Bluetooth, 802.11

### Data Link Layer

`Deal with frames`

* Detects and corrects errors
* Organizes data into `frames` before passing it down. Sequences packets (if necessary)
* Accepts acknowledgements from immediate receiver
* Examples: Ethernet MAC, PPP

An `ethernet switch` is an example of a device that works on layer 2 It forwards `ethernet frames` from one host to another as long as the hosts are connected to the switch (switches may be cascaded). 

This set of hosts and switches defines the `local area network (LAN)`.

### Network Layer

`Deals with datagrams`

From netwok to network 

* Relay and route information to destination
* Manage journey of `datagrams` and figure out intermediate hops (if needed)
* Examples: IP, X.25

An IP router is an example of a device that works on layer 3.

A router takes an incoming IP packet and determines which interface to send it out.

It enables `multiple networks` to be connected together.

### Transport Layer

`Deals with segments`

* Provides an interface for end-to-end (application-to-application) communication: sends & receives `segments` of data. Manages flow control. May include end-to-end reliability.
* Network interface is similar to a mailbox.
* Examples: TCP, UDP
