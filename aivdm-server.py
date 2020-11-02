#!./venv/bin/python3

# AIVDM Server
# Receive AIVDM (RAW NMEA ASCII) messages on UDP and forward them
# to connected TCP clients

import socket
import socketserver
import threading
import queue
import sys
#import pickle
#import select
import argparse

UDP_RECV_IP = "0.0.0.0"
TCP_ADDR = '' # all interfaces

parser = argparse.ArgumentParser(description='AIVDM Server')
parser.add_argument('-u','--receivePort', dest='udp_port', type=int, required=True,
                   help='port to listen for UDP AIVDM messages')
parser.add_argument('-l','--serverPort', dest='tcp_port', type=int, required=True,
                   help='port to listen for TCP clients to serve AIVDM messages')

args = parser.parse_args()
UDP_RECV_PORT = args.udp_port
TCP_PORT = args.tcp_port

udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udpsock.bind((UDP_RECV_IP, UDP_RECV_PORT))

# for the purpose of announcing that we are receiving UDP data
received_first_packet = False

print("Listening for UDP AIS AIVDM on %s:%s and listening for TCP client connections on %s:%s" % (UDP_RECV_IP,UDP_RECV_PORT,TCP_ADDR,TCP_PORT))

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

    # we are not handling any receive traffic
    # exceptions (including connection broken pipes and resets
    # will result in execution passing through this method and returning
    def handle(self):
        host, port = self.client_address
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

        # close the underlying socket
        self.request.close()

    def schedule(self, data):
        self.buffer.put(data)
        #self.buffer.join()

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

    # Set SO_REUSEADDR socket option prior to binding to help re-use
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

    def shut(self):
        for client in tuple(self.clients):
            client.close()


if __name__ == "__main__":

    server = ThreadedTCPServer((TCP_ADDR, TCP_PORT), ThreadedTCPRequestHandler)
    server_thread = threading.Thread(target=server.serve_forever)

    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()

    while True:

        try:
            data, addr = udpsock.recvfrom(1024) # buffer size is 1024 bytes

            if not received_first_packet and len(data):
                print("receiving UDP data from %s:%s" % addr)
                received_first_packet = True

            server.broadcast(data)

        except (KeyboardInterrupt, SystemExit):
            break
        except Exception as e:
            print(e)
            break

    print("shutting down")

    server.shut()
    server.shutdown()
    server.server_close()