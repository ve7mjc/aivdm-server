#!./venv/bin/python3

# AIVDM Server
# Receive AIVDM (RAW NMEA ASCII) messages on UDP and forward them
# to connected TCP clients

# Threading
# server (extended socketserver.TCPServer) runs in a thread
# server spawns a new thread with associated socket in a seperate thread
# 
# future functionality
# smart backfill - client receivers ~ xx minutes of back fill
# data deduction opportunities:
# only last Voyage Data (Message Type 5)
# do we want to send only the last position of each mmsi?
# only send ATON once
# opportunity to FAKE the MMSI/voyage data for ship name -- pull from a known database

import socket
import socketserver
import threading
import queue
import sys
import argparse

from datetime import datetime
import ais
import json
import traceback

# bind and listen on all interfaces
UDP_RECV_IP = "0.0.0.0" 
TCP_ADDR = ''

parser = argparse.ArgumentParser(description='AIVDM Server')
parser.add_argument('-u','--receivePort', dest='udp_port', type=int, required=True,
                   help='port to listen for UDP AIVDM messages')
parser.add_argument('-l','--serverPort', dest='tcp_port', type=int, required=True,
                   help='port to listen for TCP clients to serve AIVDM messages')
parser.add_argument('-b','--backfill', dest='backfill', type=int,
                   help='provide new client connections with a smart backfill of traffic over past specified seconds')

args = parser.parse_args()
UDP_RECV_PORT = args.udp_port
TCP_PORT = args.tcp_port

# smart backfill
backfill = False
if args.backfill:
    backfill = args.backfill
lastBackbufferPurge = datetime.timestamp(datetime.now())
stations = {}

udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# for the purpose of announcing that we are receiving UDP data
received_first_packet = False
senders = set()

def processVdm(data):
    
    try: # this method shall not disrupt operation of the application
        if args.backfill:
            
            try:
                vdm = ais.decode(data.decode("utf-8").split(',')[5], 0)
            except ais.DecodeError as e:
                return
            
            mmsi = vdm['mmsi']
            
            if mmsi not in stations:
                stations[mmsi] = {}

            stations[mmsi]['lastHeard'] = datetime.timestamp(datetime.now())
            
            if vdm['id'] == 1 or vdm['id'] == 2 or vdm['id'] == 3:
                stations[mmsi]['lastVdmPos'] = data
            if vdm['id'] == 5 or vdm['id'] == 24:
                stations[mmsi]['lastVdmStationData'] = data
            
    except Exception as e:
        print("error processing VDM: %s" % e)
        traceback.print_exc(file=sys.stdout)

def produceBackfill():
    
    buffer = b''
    curtimestamp = datetime.timestamp(datetime.now())
    
    for station in stations:
        if (stations[station]['lastHeard']+backfill) >= curtimestamp:
            if 'lastVdmPos' in stations[station]:
                buffer += stations[station]['lastVdmPos']
            if 'lastVdmVoy' in stations[station]:
                buffer += stations[station]['lastVdmStationData']
        else:
            del stations[station]
   
    return buffer

class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):

    def __init__(self, request, client_address, server):
        self.buffer = queue.Queue()
        
        # mechanism where we can signal to this request thread
        # that it must finish up
        self.requestedToClose = False
        
        super().__init__(request, client_address, server)

    def setup(self):
        super().setup()
        self.server.add_client(self) # track clients in a se

    # request is handled in this method
    # when this method returns, the thread and "request" will exit
    def handle(self):
        
        host, port = self.client_address

        if backfill:
            backdata = produceBackfill()
            print("client %s connected; sending backfill of %s bytes." % (host.rstrip(), len(backdata)))
            self.request.sendall(backdata)
        else:
            print("client %s connected" % host.rstrip())
        
        try:
            while not self.requestedToClose:
                try:
                    data = self.buffer.get(True,0.1)
                    self.request.sendall(data)
                    self.buffer.task_done()
                except (queue.Empty):
                    pass

        # different exception conditions we could log on, etc
        # for now - if we have an exception writing or reading
        # to the socket - we are going to drop this client
        except (ConnectionResetError, EOFError):
            pass
        except (BrokenPipeError):
            pass
        except Exception as e:
            pass

        self.request.close() # close the underlying socket

    # since we are operating multi-threaded, we use a thread-safe
    # queue to enqueue data to be written to the clients
    def schedule(self, data):
        self.buffer.put(data)

    # request the ceasation of work in the handle method so we
    # can do a clean shut down; closing of sockets and associated thread
    def close(self):
        self.requestedToClose = True

    def finish(self):
        host, port = self.client_address
        print("client %s disconnected" % host.rstrip())
        self.server.remove_client(self)
        super().finish()

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):

    def __init__(self, server_address, request_handler_class):
        super().__init__(server_address, request_handler_class, True)
        self.clients = set() # storage of clients

    def isShuttingdown(self):
        return self.__shutdown_request

    # Set SO_REUSEADDR socket option prior to binding to help re-use of listen port
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

    def add_client(self, client):
        self.clients.add(client)

    def broadcast(self, data):
        for client in tuple(self.clients):
            client.schedule(data)

    def remove_client(self, client):
        self.clients.remove(client)

    def shutdown(self):
        for client in tuple(self.clients):
            client.close()
        super().shutdown()

if __name__ == "__main__":

    # bind UDP listening
    try:
        udpsock.bind((UDP_RECV_IP, UDP_RECV_PORT))
    except OSError as e:
        if e.errno == 98:
            print("error: unable to bind udp/%s as it is already in use" % (UDP_RECV_PORT))
        sys.exit(1)
    except Exception as e:
        print(e)
        sys.exit(1)

    # set up overrode socketserver.TCPServer and listen on TCP port
    try:
        server = ThreadedTCPServer((TCP_ADDR, TCP_PORT), ThreadedTCPRequestHandler)
    except OSError as e:
        if e.errno == 98:
            print("error: unable to listen on tcp/%s as it is already in use" % (TCP_PORT))
        sys.exit(1)
    except Exception as e:
        print(e)
        sys.exit(1)

    server_thread = threading.Thread(target=server.serve_forever)

    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    
    server_thread.start()
    
    print("Listening for AIS VDM messages on %s udp/%s and listening for client connections on tcp/%s" % (UDP_RECV_IP,UDP_RECV_PORT,TCP_PORT))
    if backfill: print("Message backfill is enabled and set to %s seconds" % backfill)

    while True:

        try:
            # note: sockets are created in blocking mode by default
            data, addr = udpsock.recvfrom(1024)

            # announce data arrival from new host
            if addr not in senders:
                print("receiving UDP data from %s:%s" % addr)
                senders.add(addr)

            server.broadcast(data)
            processVdm(data)

        except (KeyboardInterrupt, SystemExit):
            break
        except Exception as e:
            print(e)
            break

    print("shutting down")

    server.shutdown()
    server.server_close()
    