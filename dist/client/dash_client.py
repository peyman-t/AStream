#!/usr/local/bin/python
"""
Author:            Parikshit Juluri
Contact:           pjuluri@umkc.edu
Testing:
    import dash_client
    mpd_file = <MPD_FILE>
    dash_client.playback_duration(mpd_file, 'http://198.248.242.16:8005/')

    From commandline:
    python dash_client.py -m "http://198.248.242.16:8006/media/mpd/x4ukwHdACDw.mpd" -p "all"
    python dash_client.py -m "http://127.0.0.1:8000/media/mpd/x4ukwHdACDw.mpd" -p "basic"

"""
from __future__ import division
import socket
import struct
from typing import Dict, Optional
import read_mpd
import urllib.parse as urlparse
import urllib.request as urllib2
import random
import os
import sys
import errno
import timeit
import http.client as httplib
from string import ascii_letters, digits
from argparse import ArgumentParser
from multiprocessing import Process, Queue
from queue import Empty 
from collections import defaultdict
from adaptation import basic_dash, basic_dash2, weighted_dash, netflix_dash
from adaptation.adaptation import WeightedMean
import config_dash
import dash_buffer
from configure_log_file import configure_log_file, write_json
import time
from dash_downloader import init_downloader
import concurrent.futures
import threading
import re

# To show text prompts in different colors
class COLOR:
    RESET = "\033[0m"    # Reset to default
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"

# Constants
DEFAULT_PLAYBACK = 'BASIC'
DOWNLOAD_CHUNK = 1024

# Globals for arg parser with the default values
# Not sure if this is the correct way ....
MPD = None
LIST = False
PLAYBACK = DEFAULT_PLAYBACK
DOWNLOAD = False
SEGMENT_LIMIT = None
LOG_PLAYBACK = None


class DashPlayback:
    """
    Audio[bandwidth] : {duration, url_list}
    Video[bandwidth] : {duration, url_list}
    """
    def __init__(self):

        self.min_buffer_time = None
        self.playback_duration = None
        self.audio = dict()
        self.video = dict()


def get_mpd(url):
    """ Module to download the MPD from the URL and save it to file"""
    print (url)
    try:
        connection = urllib2.urlopen(url, timeout=10)
    except urllib2.HTTPError as error:
        config_dash.LOG.error("Unable to download MPD file HTTP Error: %s" % error.code)
        return None
    except urllib2.URLError:
        error_message = "URLError. Unable to reach Server.Check if Server active"
        config_dash.LOG.error(error_message)
        print(error_message)
        return None
    except (IOError, httplib.HTTPException) as e:
        message = "Unable to , file_identifierdownload MPD file HTTP Error."
        config_dash.LOG.error(message)
        return None
    
    mpd_data = connection.read()
    connection.close()
    mpd_file = url.split('/')[-1]
    mpd_file_handle = open(mpd_file, 'w')
    mpd_file_handle.write(mpd_data.decode('utf-8'))
    mpd_file_handle.close()
    config_dash.LOG.info("Downloaded the MPD file {}".format(mpd_file))
    return mpd_file


def get_bandwidth(data, duration):
    """ Module to determine the bandwidth for a segment
    download"""
    return data * 8/duration


def get_domain_name(url):
    """ Module to obtain the domain name from the URL
        From : http://stackoverflow.com/questions/9626535/get-domain-name-from-url
    """
    parsed_uri = urlparse.urlparse(url)
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    return domain


def id_generator(id_size=6):
    """ Module to create a random string with uppercase 
        and digits.
    """
    return 'TEMP_' + ''.join(random.choice(ascii_letters+digits) for _ in range(id_size))


def download_segment(segment_url, dash_folder):
    """ Module to download the segment with download rate logging """
    try:
        # Create temp directory if it doesn't exist
        os.makedirs(dash_folder, exist_ok=True)
        
        config_dash.LOG.debug(f"Attempting to download: {segment_url}")
        
        try:
            # Add timeout and proper headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            request = urllib2.Request(segment_url, headers=headers)
            
            # Start timing the download
            download_start_time = time.time()
            connection = urllib2.urlopen(request, timeout=30)
            
        except urllib2.HTTPError as error:
            error_msg = f"HTTP Error downloading {segment_url}: {error.code}"
            config_dash.LOG.error(error_msg)
            if error.code == 404:
                config_dash.LOG.error("Segment not found - check URL construction")
            return None
            
        except urllib2.URLError as error:
            error_msg = f"URL Error downloading {segment_url}: {error.reason}"
            config_dash.LOG.error(error_msg)
            return None
            
        except Exception as error:
            error_msg = f"Error downloading {segment_url}: {str(error)}"
            config_dash.LOG.error(error_msg)
            return None

        # Parse the URL and create local path
        parsed_uri = urlparse.urlparse(segment_url)
        segment_path = parsed_uri.path.lstrip('/')
        
        # Create full path using os.path for proper handling
        segment_filename = os.path.join(dash_folder, os.path.basename(segment_path))
        
        # Create subdirectories if needed
        os.makedirs(os.path.dirname(segment_filename), exist_ok=True)
        
        # Download the segment with progress tracking
        segment_file_handle = open(segment_filename, 'wb')
        segment_size = 0
        last_log_time = time.time()
        last_size = 0
        
        try:
            while True:
                segment_data = connection.read(DOWNLOAD_CHUNK)
                if not segment_data:
                    break
                current_time = time.time()
                segment_size += len(segment_data)
                
                # Log intermediate download rate every second
                if current_time - last_log_time >= 1.0:
                    time_delta = current_time - last_log_time
                    size_delta = segment_size - last_size
                    current_rate_bps = (size_delta * 8) / time_delta
                    current_rate_MBps = (size_delta / 1024 / 1024) / time_delta
                    
                    config_dash.LOG.info(
                        f"Current download rate: {current_rate_MBps:.2f} MB/s ({current_rate_bps/1000000:.2f} Mbps)"
                    )
                    
                    last_log_time = current_time
                    last_size = segment_size
                
                segment_file_handle.write(segment_data)
                
        finally:
            connection.close()
            segment_file_handle.close()
        
        # Calculate overall download rate
        download_end_time = time.time()
        download_duration = download_end_time - download_start_time
        
        if segment_size > 0 and download_duration > 0:
            # Calculate rates
            rate_bytes_per_sec = segment_size / download_duration
            rate_bits_per_sec = (segment_size * 8) / download_duration
            rate_mbits_per_sec = rate_bits_per_sec / 1000000  # Convert to Mbps
            rate_MBps = rate_bytes_per_sec / (1024 * 1024)   # Convert to MB/s
            
            config_dash.LOG.info(
                f"Successfully downloaded: {segment_url}\n"
                f"Size: {segment_size} bytes\n"
                f"Duration: {download_duration:.2f} seconds\n"
                f"Average download rate: {rate_MBps:.2f} MB/s ({rate_mbits_per_sec:.2f} Mbps)"
            )
            
            # Store download rate in JSON handle if available
            if hasattr(config_dash, 'JSON_HANDLE'):
                if 'segment_download_rates' not in config_dash.JSON_HANDLE:
                    config_dash.JSON_HANDLE['segment_download_rates'] = []
                
                config_dash.JSON_HANDLE['segment_download_rates'].append({
                    'segment_url': segment_url,
                    'size_bytes': segment_size,
                    'duration_seconds': download_duration,
                    'rate_mbps': rate_mbits_per_sec,
                    'rate_MBps': rate_MBps,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                })
            
            return segment_size, segment_filename
        else:
            config_dash.LOG.error(f"Download completed but file size is 0 or duration is 0: {segment_url}")
            return None
        
    except Exception as e:
        error_msg = f"Error in download_segment: {str(e)}"
        config_dash.LOG.error(error_msg)
        return None

def get_tcp_info(sock: socket.socket) -> Dict[str, int]:
    """Get TCP info including CWND if available"""
    info = {}
    
    try:
        # Basic socket info
        info['recv_buffer'] = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        info['send_buffer'] = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        
        # Get TCP_INFO (Linux only)
        if hasattr(socket, 'TCP_INFO'):
            tcp_info = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 104)
            fmt = "B"*8 + "I"*24 + "Q"*7
            data = struct.unpack(fmt, tcp_info)
            info.update({
                'cwnd': data[16],            # Congestion window size
                'rtt': data[6],              # Round trip time
                'rttvar': data[7],           # RTT variance
                'snd_ssthresh': data[17],    # Slow start threshold
                'retrans': data[14],         # Number of retransmissions
                'lost': data[13],            # Segments lost
                'sacked': data[12],          # Segments SACKed
                'fackets': data[15],         # Segments FACKed
            })
    except Exception as e:
        print(f"Error getting TCP info: {e}")
    
    return info

def get_socket_from_urllib(response) -> Optional[socket.socket]:
    """Extract the underlying socket from a urllib response"""
    try:
        # For HTTP connections
        if hasattr(response.fp, 'raw'):
            if hasattr(response.fp.raw, '_sock'):
                return response.fp.raw._sock
            
        # For HTTPS connections
        if hasattr(response.fp, '_sock'):
            return response.fp._sock
            
        return None
    except AttributeError:
        return None



def get_media_all(domain, media_info, file_identifier, done_queue):
    """ Download the media from the list of URL's in media with proper process logging """
    try:
        # Configure logging for this process
        configure_log_file(playback_type="parallel")
        
        bandwidth, media_dict = media_info
        media = media_dict[bandwidth]
        media_start_time = timeit.default_timer()
        
        # Make sure initialization segment exists
        if not media.initialization:
            error_msg = f"No initialization segment for bandwidth {bandwidth}"
            config_dash.LOG.error(error_msg)
            done_queue.put((bandwidth, 'ERROR', error_msg))
            return
            
        # Create temp directory
        try:
            os.makedirs(file_identifier, exist_ok=True)
        except Exception as e:
            error_msg = f"Error creating directory {file_identifier}: {str(e)}"
            config_dash.LOG.error(error_msg)
            done_queue.put((bandwidth, 'ERROR', error_msg))
            return
            
        segments = []
        if media.initialization:
            init_url = urlparse.urljoin(domain, media.initialization)
            if not init_url.endswith('/'):
                segments.append(init_url)
                
        for segment in media.url_list:
            segment_url = urlparse.urljoin(domain, segment)
            if not segment_url.endswith('/'):
                segments.append(segment_url)
                
        config_dash.LOG.info(f"Processing {len(segments)} segments for bandwidth {bandwidth}")
        
        for segment_url in segments:
            start_time = timeit.default_timer()
            try:
                result = download_segment(segment_url, file_identifier)
                if result:
                    segment_size, segment_file = result
                    elapsed = timeit.default_timer() - start_time
                    done_queue.put((bandwidth, segment_url, elapsed))
                else:
                    error_msg = f"Failed to download segment: {segment_url}"
                    config_dash.LOG.error(error_msg)
                    continue
                    
            except Exception as e:
                error_msg = f"Error downloading segment {segment_url}: {str(e)}"
                config_dash.LOG.error(error_msg)
                continue
                
        media_download_time = timeit.default_timer() - media_start_time
        done_queue.put((bandwidth, 'STOP', media_download_time))
        
    except Exception as e:
        error_msg = f"Error in get_media_all for bandwidth {bandwidth}: {str(e)}"
        print(error_msg)  # Fallback if logging fails
        done_queue.put((bandwidth, 'ERROR', error_msg))





def make_sure_path_exists(path):
    """
    Create directory if it doesn't exist
    """
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise



def print_representations(dp_object):
    """ Module to print the representations"""
    print("The DASH media has the following video representations/bitrates")
    for bandwidth in dp_object.video:
        print (bandwidth)

def extract_last_rate(file_path):
    """Extract the last available float value X (Mbps) and count total occurrences from the log file."""
    pattern = r"Current download rate:.*\(([\d\.]+) Mbps\)"
    
    last_rate = None
    count = 0  # Counter for occurrences

    with open(file_path, 'r') as file:
        for line in file:
            match = re.search(pattern, line)
            if match:
                last_rate = float(match.group(1))  # Update last found value
                count += 1  # Increment count for every match

    return last_rate, count  # Return last value and total count

def extract_last_w_rates(file_path, w=5):
    """Extract the last `w` available float values (Mbps) from the log, starting from the most recent match."""
    pattern = r"Current download rate:.*\(([\d\.]+) Mbps\)"
    rates = []

    with open(file_path, 'r') as file:
        lines = file.readlines()  # Read all lines into memory
        for line in reversed(lines):  # Process lines from the most recent to the oldest
            match = re.search(pattern, line)
            if match:
                rates.append(float(match.group(1)))
                if len(rates) == w:  # Stop once we have `w` values
                    break

    if rates:
        return sum(rates) / len(rates)  # Return the average and count of extracted values
    return None  # Return None if no matches are found

def start_playback_smart(dp_object, domain, playback_type=None, download=False, 
                        video_segment_duration=None, use_pep=False, 
                        pep_host=None, pep_port=None, buffer_size=2*1024*1024, use_concurrent=False):
    """ Module that downloads the MPD-FIle and download
        all the representations of the Module to download
        the MPEG-DASH media.
        Example: start_playback_smart(dp_object, domain, "SMART", DOWNLOAD, video_segment_duration)

        :param dp_object: The DASH-playback object
        :param domain: The domain name of the server (The segment URLS are domain + relative_address)
        :param playback_type: The type of playback
                            1. 'BASIC' - The basic adapataion scheme
                            2. 'SARA' - Segment Aware Rate Adaptation
                            3. 'NETFLIX' - Buffer based adaptation used by Netflix
        :param download: Set to True if the segments are to be stored locally (Boolean). Default False
        :param video_segment_duration: Playback duration of each segment
        :param use_pep: Whether to use PEP proxy
        :param pep_host: PEP proxy host address
        :param pep_port: PEP proxy port
        :param buffer_size: TCP buffer size for PEP
        :param use_concurrent: Whether segments should be downloaded concurrently. Default True.
        :return:
    """
    # Initialize downloader based on playback type
    if use_pep:
        downloader = init_downloader(
            mode="pep",  # Using PEP mode for TCP-level proxy
            pep_host=pep_host,
            pep_port=pep_port,
            max_buffer_size=buffer_size
        )
    else:
        downloader = init_downloader(mode="direct")

    # Initialize the DASH buffer
    dash_player = dash_buffer.DashPlayer(dp_object.playback_duration, video_segment_duration)
    dash_player.start()

    # A folder to save the segments in
    file_identifier = id_generator()
    config_dash.LOG.info("The segments are stored in %s" % file_identifier)
    dp_list = defaultdict(defaultdict)
    
    # Creating a Dictionary of all that has the URLs for each segment and different bitrates
    for bitrate in dp_object.video:
        # Getting the URL list for each bitrate
        dp_object.video[bitrate] = read_mpd.get_url_list(dp_object.video[bitrate], video_segment_duration,
                                                         dp_object.playback_duration, bitrate)

        if "$Bandwidth$" in dp_object.video[bitrate].initialization:
            dp_object.video[bitrate].initialization = dp_object.video[bitrate].initialization.replace(
                "$Bandwidth$", str(bitrate))
        media_urls = [dp_object.video[bitrate].initialization] + dp_object.video[bitrate].url_list
        #print "media urls"
        #print media_urls
        for segment_count, segment_url in enumerate(media_urls, dp_object.video[bitrate].start):
            # segment_duration = dp_object.video[bitrate].segment_duration
            #print "segment url"
            #print segment_url
            dp_list[segment_count][bitrate] = segment_url
    bitrates = list(dp_object.video.keys())
    # bitrates = dp_object.video.keys()
    bitrates.sort()
    average_dwn_time = 0
    segment_files = []
    # For basic adaptation
    previous_segment_times = []
    recent_download_sizes = []
    weighted_mean_object = None
    current_bitrate = bitrates[0]
    previous_bitrate = None
    total_downloaded = 0
    # Delay in terms of the number of segments
    delay = 0
    segment_duration = 0
    segment_size = segment_download_time = None
    # Netflix Variables
    average_segment_sizes = netflix_rate_map = None
    netflix_state = "INITIAL"
    # Track ongoing downloads
    ongoing_downloads = []
    # Max downloads
    max_downloads = 2 if use_concurrent else 1
    # TCP rate
    parallel_dwn_rate = None
    # Use the last 'w' TCP throughput measurements appearing in the log file
    w = 5
    # Start playback of all the segments
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_downloads) as executor:  # Download multiple segments (up to "2") at the same time
        for segment_number, segment in enumerate(dp_list, dp_object.video[current_bitrate].start):
            # Wait if the concurrent download limit is reached
            config_dash.LOG.info(f"{COLOR.GREEN}{len(ongoing_downloads)} ongoing downloads out of {max_downloads}{COLOR.RESET}")
            # Wait for parallel_dwn_rate to be valid and check ongoing downloads
            while True:  # Keep checking until the conditions to break are met
                # Check if ongoing_downloads has completed
                if len(ongoing_downloads) == 0:
                    config_dash.LOG.info("All ongoing downloads are completed.")
                    break  # Exit if no ongoing downloads are left

                if use_concurrent:
                    # parallel_dwn_rate, pdrc = extract_last_rate(LOG_PLAYBACK)
                    parallel_dwn_rate = extract_last_w_rates(LOG_PLAYBACK, w)
                    if parallel_dwn_rate is not None and len(ongoing_downloads) < max_downloads:
                        parallel_dwn_rate *= 1000  # Convert to Kbps
                        config_dash.LOG.info(f"Parallel download rate: {parallel_dwn_rate} Kbps. Triggering concurrent download for next segment.")
                        break  # Exit if rate is available and less than 2 downloads are ongoing
                    break

                # Check how many downloads are still pending
                done, not_done = concurrent.futures.wait(ongoing_downloads, timeout=0.2, return_when=concurrent.futures.FIRST_COMPLETED)
                ongoing_downloads = list(not_done)

            config_dash.LOG.info(" {}: Processing the segment {}".format(playback_type.upper(), segment_number))
            write_json()
            if not previous_bitrate:
                previous_bitrate = current_bitrate
            if SEGMENT_LIMIT:
                if not dash_player.segment_limit:
                    dash_player.segment_limit = int(SEGMENT_LIMIT)
                if segment_number > int(SEGMENT_LIMIT):
                    config_dash.LOG.info("Segment limit reached")
                    break
            print ("segment_number ={}".format(segment_number))
            # print ("dp_object.video[bitrate].start={}".format(dp_object.video[bitrate].start))
            if segment_number == dp_object.video[bitrate].start:
                current_bitrate = bitrates[0]
            else:
                if playback_type.upper() == "BASIC":
                    current_bitrate, average_dwn_time = basic_dash2.basic_dash2(segment_number, bitrates, average_dwn_time,
                                                                                recent_download_sizes,
                                                                                previous_segment_times, current_bitrate, parallel_dwn_rate=parallel_dwn_rate)

                    if dash_player.buffer.qsize() > config_dash.BASIC_THRESHOLD:
                        delay = dash_player.buffer.qsize() - config_dash.BASIC_THRESHOLD
                    config_dash.LOG.info("Basic-DASH: Selected {} for the segment {}".format(current_bitrate, segment_number - 1))
                                                                                             # segment_number + 1))
                elif playback_type.upper() == "SMART":
                    if not weighted_mean_object:
                        weighted_mean_object = WeightedMean(config_dash.SARA_SAMPLE_COUNT)
                        config_dash.LOG.debug("Initializing the weighted Mean object")
                    # Checking the segment number is in acceptable range
                    if segment_number < len(dp_list) - 1 + dp_object.video[bitrate].start:
                        try:
                            current_bitrate, delay = weighted_dash.weighted_dash(bitrates, dash_player,
                                                                                 weighted_mean_object.weighted_mean_rate,
                                                                                 current_bitrate,
                                                                                 get_segment_sizes(dp_object, segment_number - 1))
                                                                                                   # segment_number+1))
                        except IndexError as e:
                            config_dash.LOG.error(e)

                elif playback_type.upper() == "NETFLIX":
                    config_dash.LOG.info("Playback is NETFLIX")
                    # Calculate the average segment sizes for each bitrate
                    if not average_segment_sizes:
                        average_segment_sizes = get_average_segment_sizes(dp_object)
                    if segment_number < len(dp_list) - 1 + dp_object.video[bitrate].start:
                        try:
                            if segment_size and segment_download_time:
                                segment_download_rate = segment_size / segment_download_time
                            else:
                                segment_download_rate = 0
                            current_bitrate, netflix_rate_map, netflix_state = netflix_dash.netflix_dash(
                                bitrates, dash_player, segment_download_rate, current_bitrate, average_segment_sizes,
                                netflix_rate_map, netflix_state)
                            config_dash.LOG.info("NETFLIX: Next bitrate = {}".format(current_bitrate))
                        except IndexError as e:
                            config_dash.LOG.error(e)
                    else:
                        config_dash.LOG.critical("Completed segment playback for Netflix")
                        break

                    # If the buffer is full wait till it gets empty
                    if dash_player.buffer.qsize() >= config_dash.NETFLIX_BUFFER_SIZE:
                        delay = (dash_player.buffer.qsize() - config_dash.NETFLIX_BUFFER_SIZE + 1) * segment_duration
                        config_dash.LOG.info("NETFLIX: delay = {} seconds".format(delay))
                else:
                    config_dash.LOG.error("Unknown playback type:{}. Continuing with basic playback".format(playback_type))
                    current_bitrate, average_dwn_time = basic_dash.basic_dash(segment_number, bitrates, average_dwn_time,
                                                                              segment_download_time, current_bitrate)
            segment_path = dp_list[segment][current_bitrate]
            segment_url = urlparse.urljoin(domain, segment_path)
            config_dash.LOG.info("{}: Segment URL = {}".format(playback_type.upper(), segment_url))
            if delay:
                delay_start = time.time()
                config_dash.LOG.info("SLEEPING for {}seconds ".format(delay*segment_duration))
                while time.time() - delay_start < (delay * segment_duration):
                    time.sleep(1)
                delay = 0
                config_dash.LOG.debug("SLEPT for {}seconds ".format(time.time() - delay_start))

            # Start downloading segment asynchronously
            def download_segment(segment_url, segment_number, current_bitrate):
                start_time = timeit.default_timer()
                try:
                    result = downloader.download_segment(segment_url, file_identifier)
                    if result:
                        segment_size, segment_filename = result
                        config_dash.LOG.info("{}: Downloaded segment {}".format(playback_type.upper(), segment_url))
                    else:
                        config_dash.LOG.error("Failed to download segment {}".format(segment_url))
                        return None
                except IOError as e:
                    config_dash.LOG.error("Unable to save segment %s" % e)
                    return None
                
                segment_download_time = timeit.default_timer() - start_time
                previous_segment_times.append(segment_download_time)
                recent_download_sizes.append(segment_size)
                
                # Update playback info
                segment_name = os.path.split(segment_url)[1]
                if "segment_info" not in config_dash.JSON_HANDLE:
                    config_dash.JSON_HANDLE["segment_info"] = list()
                config_dash.JSON_HANDLE["segment_info"].append((segment_name, current_bitrate, segment_size, segment_download_time))
                with total_downloaded_lock:  # Ensure only one thread modifies at a time
                    total_downloaded += segment_size
                config_dash.LOG.info("{} : The total downloaded = {}, segment_size = {}, segment_number = {}".format(
                    playback_type.upper(), total_downloaded, segment_size, segment_number))
                # Update weighted mean if SMART
                if playback_type.upper() == "SMART" and weighted_mean_object:
                    weighted_mean_object.update_weighted_mean(segment_size, segment_download_time)
                # Write segment info
                segment_info = {
                    'playback_length': video_segment_duration,
                    'size': segment_size,
                    'bitrate': current_bitrate,
                    'data': segment_filename,
                    'URI': segment_url,
                    'segment_number': segment_number
                }
                dash_player.write(segment_info)
                segment_files.append(segment_filename)

                config_dash.LOG.info("Downloaded %s. Size = %s in %s seconds" % (
                    segment_url, segment_size, str(segment_download_time)))

                if previous_bitrate:
                    if previous_bitrate < current_bitrate:
                        config_dash.JSON_HANDLE['playback_info']['up_shifts'] += 1
                    elif previous_bitrate > current_bitrate:
                        config_dash.JSON_HANDLE['playback_info']['down_shifts'] += 1
                    previous_bitrate = current_bitrate

                return segment_number

            future = executor.submit(download_segment, segment_url, segment_number, current_bitrate)
            ongoing_downloads.append(future)

    # Ensure all downloads complete before exiting
    concurrent.futures.wait(ongoing_downloads)

    # waiting for the player to finish playing
    while dash_player.playback_state not in dash_buffer.EXIT_STATES:
        time.sleep(1)
    write_json()
    if not download:
        clean_files(file_identifier)


# def get_segment_sizes(dp_object, segment_number):
#     """ Module to get the segment sizes for the segment_number
#     :param dp_object:
#     :param segment_number:
#     :return:
#     """
#     segment_sizes = dict([(bitrate, dp_object.video[bitrate].segment_sizes[segment_number]) for bitrate in dp_object.video])
#     config_dash.LOG.debug("The segment sizes of {} are {}".format(segment_number, segment_sizes))
#     return segment_sizes
def get_segment_sizes(dp_object, segment_number):
    """
    Module to get the segment sizes for the segment_number
    
    Args:
        dp_object: DashPlayback object containing video information
        segment_number: The segment number to get sizes for
        
    Returns:
        dict: Dictionary mapping bitrates to their segment sizes
        
    Note:
        If segment sizes are not available, estimates based on bitrate and 
        segment duration are used as fallback
    """
    segment_sizes = {}
    
    try:
        # First try to get actual segment sizes if available
        for bitrate in dp_object.video:
            media_object = dp_object.video[bitrate]
            
            # Check if we have segment_sizes data
            if hasattr(media_object, 'segment_sizes') and media_object.segment_sizes:
                try:
                    # Verify segment number is in range
                    if 0 <= segment_number < len(media_object.segment_sizes):
                        segment_sizes[bitrate] = media_object.segment_sizes[segment_number]
                        continue
                except (IndexError, TypeError) as e:
                    config_dash.LOG.debug(f"Could not get size for segment {segment_number} at bitrate {bitrate}: {e}")
            
            # Fallback: Estimate size based on bitrate and segment duration
            # Size = (bitrate * segment_duration) / 8 to convert bits to bytes
            if hasattr(media_object, 'segment_duration') and media_object.segment_duration:
                estimated_size = (bitrate * media_object.segment_duration) / 8
                segment_sizes[bitrate] = estimated_size
                config_dash.LOG.debug(f"Using estimated size for bitrate {bitrate}: {estimated_size} bytes")
            else:
                # If no segment duration, use a reasonable default (e.g., 4 seconds)
                estimated_size = (bitrate * 4) / 8
                segment_sizes[bitrate] = estimated_size
                config_dash.LOG.debug(f"Using default estimated size for bitrate {bitrate}: {estimated_size} bytes")
                
    except Exception as e:
        config_dash.LOG.error(f"Error getting segment sizes: {e}")
        # Provide minimal fallback to prevent adaptation failure
        for bitrate in dp_object.video:
            segment_sizes[bitrate] = bitrate  # Use bitrate as minimal fallback

    if not segment_sizes:
        config_dash.LOG.warning("No segment sizes available, using bitrates as sizes")
        segment_sizes = dict((bitrate, bitrate) for bitrate in dp_object.video)
        
    config_dash.LOG.debug(f"Segment sizes for segment {segment_number}: {segment_sizes}")
    return segment_sizes

def get_average_segment_sizes(dp_object):
    """
    Module to get the avearge segment sizes for each bitrate
    :param dp_object:
    :return: A dictionary of aveage segment sizes for each bitrate
    """
    average_segment_sizes = dict()
    for bitrate in dp_object.video:
        segment_sizes = dp_object.video[bitrate].segment_sizes
        segment_sizes = [float(i) for i in segment_sizes]
        try:
            average_segment_sizes[bitrate] = sum(segment_sizes)/len(segment_sizes)
        except ZeroDivisionError:
            average_segment_sizes[bitrate] = 0
    config_dash.LOG.info("The avearge segment size for is {}".format(average_segment_sizes.items()))
    return average_segment_sizes


def clean_files(folder_path):
    """
    :param folder_path: Local Folder to be deleted
    """
    if os.path.exists(folder_path):
        try:
            for video_file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, video_file)
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            os.rmdir(folder_path)
        except (OSError) as e:
            config_dash.LOG.info("Unable to delete the folder {}. {}".format(folder_path, e))
        config_dash.LOG.info("Deleted the folder '{}' and its contents".format(folder_path))


def start_playback_all(dp_object, domain, video_segment_duration):
    """ Module that downloads the MPD-FIle and download all the representations """
    # Create temp directory
    file_identifier = f'TEMP_{id_generator(6)}'
    
    try:
        # Create the directory in the main process
        os.makedirs(file_identifier, exist_ok=True)
        config_dash.LOG.info(f"Created temporary directory: {file_identifier}")
    except Exception as e:
        config_dash.LOG.error(f"Failed to create temporary directory: {e}")
        return None
    
    video_done_queue = Queue()
    processes = []
    
    config_dash.LOG.info("File Segments are in %s" % file_identifier)

    # Start download processes
    for bitrate in dp_object.video:
        dp_object.video[bitrate] = read_mpd.get_url_list(dp_object.video[bitrate],
                                                        dp_object.video[bitrate].segment_duration,
                                                        dp_object.playback_duration,
                                                        bitrate)
        process = Process(target=get_media_all, 
                        args=(domain, (bitrate, dp_object.video),
                              file_identifier, video_done_queue))
        process.daemon = True  # Make process daemon to ensure cleanup
        process.start()
        processes.append(process)

    try:
        # Wait for all processes with timeout
        for process in processes:
            process.join(timeout=300)  # 5 minute timeout per process
            
        count = 0
        while count < len(dp_object.video):
            try:
                queue_values = video_done_queue.get(timeout=60)
                bitrate, status, info = queue_values
                
                if status == 'ERROR':
                    config_dash.LOG.error(f"Error downloading bitrate {bitrate}: {info}")
                elif status == 'STOP':
                    config_dash.LOG.info(f"Completed download of {bitrate} in {info} seconds")
                    count += 1
                    
            except Empty:
                config_dash.LOG.error("Timeout waiting for download completion")
                break
                
    except Exception as e:
        config_dash.LOG.error(f"Error in download processes: {str(e)}")
    finally:
        # Cleanup processes
        for process in processes:
            if process.is_alive():
                process.terminate()





def create_arguments(parser):
    """ Adding arguments to the parser """
    parser.add_argument('-m', '--MPD',                   
                        help="Url to the MPD File")
    parser.add_argument('-l', '--LIST', action='store_true',
                        help="List all the representations")
    parser.add_argument('-p', '--PLAYBACK',
                        default=DEFAULT_PLAYBACK,
                        help="Playback type (basic, sara, netflix, or all)")
    parser.add_argument('-n', '--SEGMENT_LIMIT',
                        default=SEGMENT_LIMIT,
                        help="The Segment number limit")
    parser.add_argument('-d', '--DOWNLOAD', action='store_true',
                        default=False,
                        help="Keep the video files after playback")
    # New arguments for PEP configuration
    parser.add_argument('--use-pep', action='store_true',
                        help="Use Performance Enhancing Proxy")
    parser.add_argument('--pep-host', default='localhost',
                        help="PEP proxy host address")
    parser.add_argument('--pep-port', type=int, default=8888,
                        help="PEP proxy port")
    parser.add_argument('--buffer-size', type=int, default=2*1024*1024,
                        help="TCP receive buffer size in bytes for PEP")
    parser.add_argument('--use-concurrent', action='store_true', default=False,
                        help="Download multiple segments (at most 2) concurrently. Default False.")


def main():
    """ Main Program wrapper """
    # Create arguments
    parser = ArgumentParser(description='Process Client parameters')
    create_arguments(parser)
    args = parser.parse_args()

    global LOG_PLAYBACK
    
    # Instead of using globals(), access args directly
    LOG_PLAYBACK = configure_log_file(playback_type=args.PLAYBACK.lower())
    print(f"LOG_PLAYBACK: {LOG_PLAYBACK}")
    config_dash.JSON_HANDLE['playback_type'] = args.PLAYBACK.lower()
    
    if not args.MPD:
        print("ERROR: Please provide the URL to the MPD file. Try Again..")
        return None
    
    mpd_url = args.MPD  # Store MPD URL in a variable
    config_dash.LOG.info('Downloading MPD file %s' % mpd_url)
    
    # Get the base URL path
    base_path = mpd_url.rsplit('/', 1)[0] + '/'
    
    if args.use_pep:
        config_dash.LOG.info(f'Using PEP proxy at {args.pep_host}:{args.pep_port}')
    
    # Retrieve the MPD files for the video
    mpd_file = get_mpd(mpd_url)
    if not mpd_file:
        config_dash.LOG.error("Failed to download MPD file")
        return None
        
    domain = base_path  
    dp_object = DashPlayback()
    
    try:
        # Reading the MPD file
        dp_object, video_segment_duration = read_mpd.read_mpd(mpd_file, dp_object, mpd_url)
        if not dp_object.video:
            config_dash.LOG.error("No video representations found in MPD file")
            return None
            
        config_dash.LOG.info("The DASH media has %d video representations" % len(dp_object.video))
        
        if args.LIST:
            print_representations(dp_object)
            return None

        try:
            buffer_size = getattr(args, 'buffer_size', 1024*1024)  # Default 1MB if not specified
            use_concurrent = getattr(args, 'use_concurrent', False)  # Default False if not specified
            
            if "all" in args.PLAYBACK.lower():
                if mpd_file:
                    config_dash.LOG.critical("Start ALL Parallel PLayback")
                    start_playback_all(dp_object, domain, video_segment_duration)
            elif "basic" in args.PLAYBACK.lower():
                config_dash.LOG.critical("Started Basic-DASH Playback")
                start_playback_smart(dp_object, domain, "BASIC", args.DOWNLOAD, video_segment_duration,
                                   use_pep=args.use_pep, pep_host=args.pep_host, 
                                   pep_port=args.pep_port, buffer_size=buffer_size, use_concurrent=use_concurrent)
            elif "sara" in args.PLAYBACK.lower():
                config_dash.LOG.critical("Started SARA-DASH Playback")
                start_playback_smart(dp_object, domain, "SMART", args.DOWNLOAD, video_segment_duration,
                                   use_pep=args.use_pep, pep_host=args.pep_host, 
                                   pep_port=args.pep_port, buffer_size=buffer_size, use_concurrent=use_concurrent)
            elif "netflix" in args.PLAYBACK.lower():
                config_dash.LOG.critical("Started Netflix-DASH Playback")
                start_playback_smart(dp_object, domain, "NETFLIX", args.DOWNLOAD, video_segment_duration,
                                   use_pep=args.use_pep, pep_host=args.pep_host, 
                                   pep_port=args.pep_port, buffer_size=buffer_size, use_concurrent=use_concurrent)
            else:
                config_dash.LOG.error("Unknown Playback parameter {}".format(args.PLAYBACK))
                return None
                
        except KeyboardInterrupt:
            config_dash.LOG.info("Playback interrupted by user")
            return None
        except Exception as e:
            config_dash.LOG.error(f"Error during playback: {str(e)}")
            return None
            
    except Exception as e:
        config_dash.LOG.error(f"Error parsing MPD file: {str(e)}")
        return None


if __name__ == "__main__":
    sys.exit(main())
