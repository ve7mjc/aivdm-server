# AIVDM-Server

Provide an AIS VDM (AIVDM) feed to TCP connected clients.

- Receive VDM messages from one or more senders on a specified UDP port
- Mirror messages received on UDP to connected TCP clients as-is

This can be used in conjunction with AISHub aisdispatcher to enable real-time chart plotting and feeds to other software which expects a direct connection to an AIS receiver.

## Requirements

Python 3.x

