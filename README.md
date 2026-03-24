Dashboard for MMDVM using MQTT protocol developed by FReeDMR IT Dev Team.

This project uses Jonathan G4KLX's latest implementations of the MMDVM suite.

Currently, the entire logging system is managed via the MQTT protocol, so a "broker" like Mosquitto must be installed to route the log streams.

Topics must be in the format mmdvm/<node-name> for MMDVMHost.ini

For <MODE>Gateway.ini, the topic format must be: <mode>-gateway/<nodename>

You can find an example here: http://mqtt.freedmr.it:7000
