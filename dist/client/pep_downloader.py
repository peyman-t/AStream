import socket
import select
import queue
import threading
import time
import os
import logging
import urllib.parse as urlparse
import urllib.request as urllib2
from typing import Optional, Tuple, Dict

class PEPDownloader:
    """Performance Enhancing Proxy behavior for DASH segment downloads"""
    def __init__(self, max_buffer_size: int = 1024*1024,  # 1MB default
                 pep_host: str = None,
                 pep_port: int = None):
        self.max_buffer_size = max_buffer_size
        self.pep_host = pep_host
        self.pep_port = pep_port
        self.pending_requests = queue.Queue()
        self.active_downloads = {}
        self.lock = threading.Lock()
        self.download_chunk = 8192  # 8KB chunks
        
    def configure_socket(self, sock: socket.socket) -> None:
        """Configure socket buffer sizes and TCP options"""
        try:
            # Set maximum receive buffer size
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.max_buffer_size)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.max_buffer_size)
            
            # Enable TCP_NODELAY to prevent buffering (disable Nagle's algorithm)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            
            # Set TCP_QUICKACK for faster acknowledgments if available
            if hasattr(socket, 'TCP_QUICKACK'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_QUICKACK, 1)
                
            # Enable TCP window scaling if available
            if hasattr(socket, 'TCP_WINDOW_CLAMP'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_WINDOW_CLAMP, 0)
                
            # Set keepalive options
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, 'TCP_KEEPIDLE'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            if hasattr(socket, 'TCP_KEEPINTVL'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            if hasattr(socket, 'TCP_KEEPCNT'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6)
                
        except Exception as e:
            logging.error(f"Error configuring socket: {e}")

    def _create_opener(self) -> urllib2.OpenerDirector:
        """Create URL opener with appropriate proxy settings"""
        if self.pep_host and self.pep_port:
            proxy_handler = urllib2.ProxyHandler({
                'http': f'http://{self.pep_host}:{self.pep_port}',
                'https': f'http://{self.pep_host}:{self.pep_port}'
            })
            return urllib2.build_opener(proxy_handler)
        return urllib2.build_opener()

    def _prepare_download(self, segment_url: str, dash_folder: str) -> Tuple[str, str]:
        """Prepare download paths"""
        # Create temp directory and necessary subdirectories
        os.makedirs(dash_folder, exist_ok=True)
        
        # Parse URL and create local path
        parsed_uri = urlparse.urlparse(segment_url)
        segment_path = parsed_uri.path.lstrip('/')
        segment_filename = os.path.join(dash_folder, os.path.basename(segment_path))
        
        # Ensure subdirectories exist
        os.makedirs(os.path.dirname(segment_filename), exist_ok=True)
        
        return segment_path, segment_filename

    def _get_socket_from_connection(self, connection) -> Optional[socket.socket]:
        """Extract underlying socket from urllib connection"""
        try:
            if hasattr(connection.fp, 'raw'):
                if hasattr(connection.fp.raw, '_sock'):
                    return connection.fp.raw._sock
            elif hasattr(connection.fp, '_sock'):
                return connection.fp._sock
            return None
        except Exception:
            return None

    def _log_tcp_info(self, sock: socket.socket):
        """Log TCP connection information if available"""
        try:
            if hasattr(sock, 'getsockopt') and hasattr(socket, 'TCP_INFO'):
                # This is Linux-specific TCP info
                tcp_info = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 92)
                # We only log if successful, no need for else
                logging.debug(f"TCP Info available: {len(tcp_info)} bytes")
        except Exception:
            pass  # TCP_INFO not available, skip silently

    def download_segment_pep(self, segment_url: str, dash_folder: str) -> Optional[Tuple[int, float]]:
        """Enhanced segment download with PEP-like behavior"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (DASH Client with PEP behavior)',
                'Connection': 'keep-alive'
            }
            
            proxy_handler = urllib2.ProxyHandler({
                'http': f'http://{self.pep_host}:{self.pep_port}',
                'https': f'http://{self.pep_host}:{self.pep_port}'
            })
            
            # Build opener with proxy support
            opener = urllib2.build_opener(proxy_handler)
            urllib2.install_opener(opener)
            
            request = urllib2.Request(segment_url, headers=headers)
            segment_path, segment_filename = self._prepare_download(segment_url, dash_folder)
            start_time = time.time()
            
            with opener.open(request, timeout=30) as connection:
                # Get and configure underlying socket if possible
                sock = self._get_socket_from_connection(connection)
                if sock:
                    self.configure_socket(sock)
                    self._log_tcp_info(sock)
                
                # Download the segment with progress tracking
                segment_size = 0
                last_log_time = time.time()
                last_size = 0
                
                with open(segment_filename, 'wb') as segment_file:
                    while True:
                        try:
                            data = connection.read(self.download_chunk)
                            if not data:
                                break
                                
                            current_time = time.time()
                            segment_size += len(data)
                            segment_file.write(data)
                            
                            # Log progress every second
                            if current_time - last_log_time >= 1.0:
                                duration = current_time - last_log_time
                                size_delta = segment_size - last_size
                                current_rate_mbps = (size_delta * 8) / (duration * 1000000)
                                
                                logging.info(
                                    f"Downloading {segment_url}\n"
                                    f"Progress: {segment_size} bytes\n"
                                    f"Current rate: {current_rate_mbps:.2f} Mbps"
                                )
                                
                                last_log_time = current_time
                                last_size = segment_size
                        except Exception as e:
                            logging.error(f"Error reading data: {e}")
                            break
                
                download_time = time.time() - start_time
                
                if segment_size > 0:
                    average_rate_mbps = (segment_size * 8) / (download_time * 1000000)
                    logging.info(
                        f"Downloaded {segment_url}\n"
                        f"Size: {segment_size} bytes\n"
                        f"Time: {download_time:.2f} s\n"
                        f"Average rate: {average_rate_mbps:.2f} Mbps"
                    )
                    
                    return segment_size, download_time
                    
        except Exception as e:
            logging.error(f"Error in PEP download: {e}")
            return None


    def cleanup(self):
        """Cleanup resources"""
        try:
            with self.lock:
                for download_info in self.active_downloads.values():
                    if 'socket' in download_info:
                        try:
                            download_info['socket'].close()
                        except Exception:
                            pass
                self.active_downloads.clear()
        except Exception as e:
            logging.error(f"Error in cleanup: {e}")