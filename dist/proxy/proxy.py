import socket
import ssl
import select
import threading
import logging
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DASH-PEP')

class HTTPSConnectionHandler:
    def __init__(self, client_sock: socket.socket, client_addr: Tuple[str, int]):
        self.client_sock = client_sock
        self.client_addr = client_addr
        self.server_sock: Optional[socket.socket] = None
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.buffer_size = 64 * 1024  # 64KB buffer
        self.running = False

    def connect_to_server(self, host: str, port: int = 443) -> bool:
        """Establish SSL connection to the origin server"""
        try:
            # Create TCP socket
            plain_socket = socket.create_connection((host, port))
            
            # Wrap with SSL
            self.server_sock = self.ssl_context.wrap_socket(
                plain_socket, 
                server_hostname=host
            )
            return True
        except Exception as e:
            logger.error(f"Failed to connect to server {host}:{port}: {e}")
            return False

    def optimize_connections(self):
        """Apply TCP optimizations to both connections"""
        for sock in [self.client_sock, self.server_sock]:
            if sock:
                try:
                    # Increase buffer sizes
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256 * 1024)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 256 * 1024)
                    
                    # Disable Nagle's algorithm
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    
                    # Enable TCP Quick ACK if available
                    if hasattr(socket, 'TCP_QUICKACK'):
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_QUICKACK, 1)
                except Exception as e:
                    logger.error(f"Error optimizing socket: {e}")

    def proxy_data(self):
        """Handle proxying data between client and server with improved SSL handling"""
        try:
            # Initialize buffers for SSL data
            client_buffer = b''
            server_buffer = b''
            
            while self.running:
                # Wait for data on either socket with timeout
                readable, writeable, _ = select.select(
                    [self.client_sock, self.server_sock],  # Read sockets
                    [],  # Write sockets
                    [],  # Exception sockets
                    1.0   # Timeout
                )

                for sock in readable:
                    # Determine which direction we're forwarding
                    if sock == self.client_sock:
                        source_sock = self.client_sock
                        dest_sock = self.server_sock
                        buffer_name = "client"
                    else:
                        source_sock = self.server_sock
                        dest_sock = self.client_sock
                        buffer_name = "server"

                    try:
                        # Try to receive data
                        data = sock.recv(self.buffer_size)
                        
                        if data:
                            try:
                                # Forward data to the other endpoint
                                dest_sock.sendall(data)
                                logger.debug(f"Forwarded {len(data)} bytes from {buffer_name}")
                            except (ssl.SSLWantWriteError, ssl.SSLWantReadError):
                                # SSL operations would block - retry
                                continue
                            except Exception as e:
                                logger.error(f"Error forwarding data from {buffer_name}: {e}")
                                self.running = False
                                break
                        else:
                            # A empty recv() usually means the connection was closed
                            # However, for SSL we need to check if it's just a session closure
                            if isinstance(sock, ssl.SSLSocket):
                                try:
                                    # Try to read any remaining SSL data
                                    sock.recv(1)
                                except ssl.SSLWantReadError:
                                    # More data might be coming
                                    continue
                                except (ssl.SSLError, ssl.SSLEOFError):
                                    # True SSL session end
                                    logger.info(f"{buffer_name} SSL session ended")
                                    self.running = False
                                    break
                            else:
                                # Non-SSL socket with no data means connection closed
                                logger.info(f"{buffer_name} connection closed")
                                self.running = False
                                break
                                
                    except ssl.SSLWantReadError:
                        # SSL read would block - normal for non-blocking sockets
                        continue
                    except ssl.SSLError as e:
                        logger.error(f"SSL error on {buffer_name}: {e}")
                        self.running = False
                        break
                    except ConnectionError as e:
                        logger.error(f"Connection error on {buffer_name}: {e}")
                        self.running = False
                        break
                    except Exception as e:
                        logger.error(f"Error receiving from {buffer_name}: {e}")
                        self.running = False
                        break

        except Exception as e:
            logger.error(f"Error in proxy_data: {e}")
            self.running = False
        finally:
            logger.info("Ending proxy data forwarding")

    def handle_client(self):
        """Main client handling loop"""
        try:
            # Read initial request to get host
            data = self.client_sock.recv(self.buffer_size)
            if not data:
                logger.error("No data received from client")
                return
                
            try:
                connect_request = data.decode('utf-8').strip()
            except UnicodeDecodeError:
                logger.error("Invalid request encoding")
                return
                
            if connect_request.startswith('CONNECT'):
                # Parse: "CONNECT dash.akamaized.net:443 HTTP/1.0"
                try:
                    _, target_host_port, _ = connect_request.split(' ')
                    target_host, target_port = target_host_port.split(':')
                    target_port = int(target_port)
                    
                    logger.info(f"Connecting to {target_host}:{target_port}")
                    
                    # Connect to actual target
                    if self.connect_to_server(target_host, target_port):
                        # Apply optimizations
                        self.optimize_connections()
                        
                        # Send 200 Connection Established back to client
                        response = "HTTP/1.1 200 Connection established\r\n\r\n"
                        self.client_sock.sendall(response.encode())
                        
                        # Start proxying data
                        self.running = True
                        self.proxy_data()
                    else:
                        logger.error("Failed to connect to target server")
                        response = "HTTP/1.1 502 Bad Gateway\r\n\r\n"
                        self.client_sock.sendall(response.encode())
                except ValueError:
                    logger.error("Invalid CONNECT request format")
                    response = "HTTP/1.1 400 Bad Request\r\n\r\n"
                    self.client_sock.sendall(response.encode())
            else:
                logger.error("Expected CONNECT request, got something else")
                response = "HTTP/1.1 405 Method Not Allowed\r\n\r\n"
                self.client_sock.sendall(response.encode())
                
        except Exception as e:
            logger.error(f"Error in handle_client: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up connections"""
        try:
            self.client_sock.close()
            if self.server_sock:
                self.server_sock.close()
        except Exception as e:
            logger.error(f"Error in cleanup: {e}")

class DashHTTPSProxy:
    def __init__(self, listen_host: str = '0.0.0.0', listen_port: int = 8888,
                 target_host: str = 'dash.akamaized.net', target_port: int = 443):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.running = False
        self.connections = []

    def start(self):
        """Start the proxy server"""
        try:
            self.sock.bind((self.listen_host, self.listen_port))
            self.sock.listen(100)
            self.running = True
            
            logger.info(f"DASH HTTPS Proxy listening on {self.listen_host}:{self.listen_port}")
            logger.info(f"Forwarding to {self.target_host}:{self.target_port}")

            while self.running:
                try:
                    client_sock, client_addr = self.sock.accept()
                    logger.info(f"New connection from {client_addr}")

                    handler = HTTPSConnectionHandler(client_sock, client_addr)
                    thread = threading.Thread(target=handler.handle_client)
                    thread.daemon = True
                    thread.start()
                    self.connections.append(thread)

                except Exception as e:
                    logger.error(f"Error accepting connection: {e}")

        except Exception as e:
            logger.error(f"Error starting proxy: {e}")
        finally:
            self.cleanup()

    def stop(self):
        """Stop the proxy server"""
        self.running = False
        try:
            # Create a connection to unblock accept()
            socket.create_connection((self.listen_host, self.listen_port))
        except Exception:
            pass

    def cleanup(self):
        """Clean up all connections"""
        try:
            self.sock.close()
            for thread in self.connections:
                thread.join(timeout=1.0)
        except Exception as e:
            logger.error(f"Error in cleanup: {e}")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='DASH HTTPS Proxy')
    parser.add_argument('--listen-host', default='0.0.0.0', help='Listen host')
    parser.add_argument('--listen-port', type=int, default=8888, help='Listen port')
    parser.add_argument('--target-host', default='dash.akamaized.net', help='Target host')
    parser.add_argument('--target-port', type=int, default=443, help='Target port')
    
    args = parser.parse_args()

    proxy = DashHTTPSProxy(
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        target_host=args.target_host,
        target_port=args.target_port
    )

    try:
        proxy.start()
    except KeyboardInterrupt:
        logger.info("Stopping proxy...")
        proxy.stop()