import os
import time
import urllib.parse as urlparse
import urllib.request as urllib2
from typing import Optional, Tuple, Dict
from enum import Enum
import config_dash
from pep_downloader import PEPDownloader

class DownloadMode(Enum):
    DIRECT = "direct"
    PEP = "pep"

class DASHDownloader:
    """Unified downloader supporting both direct and PEP-enhanced downloads"""
    
    def __init__(self, mode: DownloadMode = DownloadMode.DIRECT, 
                 pep_host: str = None,
                 pep_port: int = None,
                 max_buffer_size: int = 1024*1024,  # 1MB default
                 download_chunk: int = 1024):
        self.mode = mode
        self.download_chunk = download_chunk
        self.pep_host = pep_host
        self.pep_port = pep_port
        self.pep_downloader = None
        
        if mode == DownloadMode.PEP and pep_host and pep_port:
            self.pep_downloader = PEPDownloader(
                max_buffer_size=max_buffer_size,
                pep_host=pep_host,
                pep_port=pep_port
            )

    def _prepare_download(self, segment_url: str, dash_folder: str) -> Tuple[str, str]:
        """Common preparation for both download methods"""
        # Create temp directory and necessary subdirectories
        os.makedirs(dash_folder, exist_ok=True)
        
        # Parse URL and create local path
        parsed_uri = urlparse.urlparse(segment_url)
        segment_path = parsed_uri.path.lstrip('/')
        segment_filename = os.path.join(dash_folder, os.path.basename(segment_path))
        
        # Ensure subdirectories exist
        os.makedirs(os.path.dirname(segment_filename), exist_ok=True)
        
        return segment_path, segment_filename

    def _create_opener(self) -> urllib2.OpenerDirector:
        """Create URL opener with appropriate proxy settings"""
        if self.mode == DownloadMode.PEP and self.pep_host and self.pep_port:
            proxy_handler = urllib2.ProxyHandler({
                'http': f'http://{self.pep_host}:{self.pep_port}',
                'https': f'http://{self.pep_host}:{self.pep_port}'
            })
            return urllib2.build_opener(proxy_handler)
        return urllib2.build_opener()

    def _log_download_stats(self, segment_url: str, segment_size: int, 
                           download_duration: float) -> None:
        """Log download statistics and update JSON handle"""
        if segment_size > 0 and download_duration > 0:
            # Calculate rates
            rate_bytes_per_sec = segment_size / download_duration
            rate_bits_per_sec = (segment_size * 8) / download_duration
            rate_mbits_per_sec = rate_bits_per_sec / 1000000
            rate_MBps = rate_bytes_per_sec / (1024 * 1024)
            
            config_dash.LOG.info(
                f"Successfully downloaded: {segment_url}\n"
                f"Size: {segment_size} bytes\n"
                f"Duration: {download_duration:.2f} seconds\n"
                f"Average download rate: {rate_MBps:.2f} MB/s ({rate_mbits_per_sec:.2f} Mbps)"
            )
            
            # Update JSON handle if available
            if hasattr(config_dash, 'JSON_HANDLE'):
                if 'segment_download_rates' not in config_dash.JSON_HANDLE:
                    config_dash.JSON_HANDLE['segment_download_rates'] = []
                    
                config_dash.JSON_HANDLE['segment_download_rates'].append({
                    'segment_url': segment_url,
                    'size_bytes': segment_size,
                    'duration_seconds': download_duration,
                    'rate_mbps': rate_mbits_per_sec,
                    'rate_MBps': rate_MBps,
                    'download_mode': self.mode.value,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                })

    def _download_segment_direct(self, segment_url: str, dash_folder: str) -> Optional[Tuple[int, str]]:
        """Direct download implementation"""
        try:
            config_dash.LOG.debug(f"Direct download: {segment_url}")
            segment_path, segment_filename = self._prepare_download(segment_url, dash_folder)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            request = urllib2.Request(segment_url, headers=headers)
            opener = self._create_opener()
            
            download_start_time = time.time()
            
            with opener.open(request, timeout=30) as connection:
                with open(segment_filename, 'wb') as segment_file:
                    segment_size = 0
                    last_log_time = time.time()
                    last_size = 0
                    
                    while True:
                        segment_data = connection.read(self.download_chunk)
                        if not segment_data:
                            break
                            
                        current_time = time.time()
                        segment_size += len(segment_data)

                        log_frequency = 0.1 # seconds after which the throughput will be logged
                        
                        # Log intermediate rates every second
                        if current_time - last_log_time >= log_frequency: # 1.0:
                            time_delta = current_time - last_log_time
                            size_delta = segment_size - last_size
                            current_rate_bps = (size_delta * 8) / time_delta
                            current_rate_MBps = (size_delta / 1024 / 1024) / time_delta
                            
                            config_dash.LOG.info(
                                f"Current download rate: {current_rate_MBps:.2f} MB/s "
                                f"({current_rate_bps/1000000:.2f} Mbps)"
                            )
                            
                            last_log_time = current_time
                            last_size = segment_size
                            
                        segment_file.write(segment_data)
                        
            download_duration = time.time() - download_start_time
            self._log_download_stats(segment_url, segment_size, download_duration)
            
            return segment_size, segment_filename
            
        except urllib2.HTTPError as error:
            config_dash.LOG.error(f"HTTP Error downloading {segment_url}: {error.code}")
            return None
        except urllib2.URLError as error:
            config_dash.LOG.error(f"URL Error downloading {segment_url}: {error.reason}")
            return None
        except Exception as e:
            config_dash.LOG.error(f"Error in direct download: {str(e)}")
            return None

    def _download_segment_pep(self, segment_url: str, dash_folder: str) -> Optional[Tuple[int, str]]:
        """PEP-enhanced download implementation"""
        try:
            config_dash.LOG.debug(f"PEP download: {segment_url}")
            segment_path, segment_filename = self._prepare_download(segment_url, dash_folder)
            
            if not self.pep_downloader:
                config_dash.LOG.error("PEP downloader not initialized")
                return None
                
            # Use PEPDownloader's method for the actual download
            result = self.pep_downloader.download_segment_pep(segment_url, dash_folder)
            
            if result:
                segment_size, _ = result  # PEP downloader returns size, url
                download_time = 0  # Get actual download time from PEP downloader
                self._log_download_stats(segment_url, segment_size, download_time)
                return segment_size, segment_filename
                
            return None
            
        except Exception as e:
            config_dash.LOG.error(f"Error in PEP download: {e}")
            return None

    def download_segment(self, segment_url: str, dash_folder: str) -> Optional[Tuple[int, str]]:
        """Download segment using selected mode"""
        if self.mode == DownloadMode.PEP and self.pep_downloader:
            return self._download_segment_pep(segment_url, dash_folder)
        return self._download_segment_direct(segment_url, dash_folder)
        

def init_downloader(mode: str = "direct", 
                  pep_host: str = None, 
                  pep_port: int = None,
                  max_buffer_size: int = 1024*1024) -> DASHDownloader:
    """Initialize downloader with specified mode and proxy settings"""
    try:
        download_mode = DownloadMode(mode.lower())
        return DASHDownloader(
            mode=download_mode, 
            pep_host=pep_host,
            pep_port=pep_port,
            max_buffer_size=max_buffer_size
        )
    except ValueError:
        config_dash.LOG.warning(f"Invalid mode '{mode}', falling back to direct download")
        return DASHDownloader(mode=DownloadMode.DIRECT)
