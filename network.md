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
