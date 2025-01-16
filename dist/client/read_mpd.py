"""
Module for reading the MPD file
Author: Parikshit Juluri
Contact : pjuluri@umkc.edu
Modified for improved error handling and robustness
"""

from __future__ import division
import re
import os
import logging
import config_dash
from typing import Optional, Tuple, Dict, List, Union
import xml.etree.ElementTree as ET

# Constants
FORMAT = 0
URL_LIST: List[str] = []

# Dictionary to convert size to bits
SIZE_DICT = {
    'bits':  1,
    'Kbits': 1024,
    'Mbits': 1024*1024,
    'bytes': 8,
    'KB':    1024*8,
    'MB':    1024*1024*8,
}

MEDIA_PRESENTATION_DURATION = 'mediaPresentationDuration'
MIN_BUFFER_TIME = 'minBufferTime'

def get_tag_name(xml_element: str) -> Optional[str]:
    """
    Remove the xmlns tag from the name
    Args:
        xml_element: XML element tag with potential xmlns
    Returns:
        Clean tag name or None if processing fails
    """
    try:
        if not isinstance(xml_element, str):
            return None
        index = xml_element.find('}')
        if index == -1:
            return xml_element
        return xml_element[index + 1:]
    except Exception as e:
        config_dash.LOG.error(f"Error in get_tag_name: {e}")
        return None

def get_playback_time(playback_duration: str) -> float:
    """
    Get the playback time (in seconds) from duration string
    Args:
        playback_duration: Duration string (e.g., "PT0H1M59.89S")
    Returns:
        Duration in seconds
    """
    try:
        if not playback_duration:
            return 0.0
        
        # Get all the numbers in the string
        numbers = re.split('[PTHMS]', playback_duration)
        # remove all the empty strings
        numbers = [float(value) for value in numbers if value]
        numbers.reverse()
        
        # Calculate total duration
        multipliers = [1, 60, 3600]  # seconds, minutes, hours
        return sum(n * m for n, m in zip(numbers, multipliers[:len(numbers)]))
    except Exception as e:
        config_dash.LOG.error(f"Error parsing playback time '{playback_duration}': {e}")
        return 0.0

class MediaObject:
    """Object to handle audio and video stream information"""
    def __init__(self):
        self.min_buffer_time = None
        self.start = None
        self.timescale = None
        self.segment_duration = None
        self.initialization = None
        self.base_url = None
        self.base_url_path = None
        self.url_list = []
        self.segment_sizes = []


class DashPlayback:
    """
    Container for audio/video playback information
    Audio[bandwidth] : {duration, url_list}
    Video[bandwidth] : {duration, url_list}
    """
    def __init__(self):
        self.min_buffer_time: Optional[float] = None
        self.playback_duration: Optional[float] = None
        self.audio: Dict[int, MediaObject] = {}
        self.video: Dict[int, MediaObject] = {}

def ensure_metadata_structure() -> None:
    """Ensure the JSON handle has the required structure"""
    try:
        if not hasattr(config_dash, 'JSON_HANDLE'):
            config_dash.JSON_HANDLE = {}
        
        if 'video_metadata' not in config_dash.JSON_HANDLE:
            config_dash.JSON_HANDLE['video_metadata'] = {}
            
        if 'available_bitrates' not in config_dash.JSON_HANDLE['video_metadata']:
            config_dash.JSON_HANDLE['video_metadata']['available_bitrates'] = []
    except Exception as e:
        config_dash.LOG.error(f"Error ensuring metadata structure: {e}")

def process_segment_info(segment_info: ET.Element, media_object: MediaObject, 
                        bandwidth: int, cut_url: str) -> Optional[float]:
    """
    Process segment information from the MPD
    Returns segment duration if found
    """
    try:
        tag_name = get_tag_name(segment_info.tag)
        if not tag_name:
            return None

        if "SegmentTemplate" in tag_name:
            media_object.base_url = segment_info.attrib.get('media', '')
            media_object.start = int(segment_info.attrib.get('startNumber', '1'))
            media_object.timescale = float(segment_info.attrib.get('timescale', '1'))
            
            if 'duration' in segment_info.attrib:
                return float(segment_info.attrib['duration']) / media_object.timescale
            
        elif "SegmentBase" in tag_name:
            for init in segment_info:
                if 'sourceURL' in init.attrib:
                    media_object.initialization = cut_url + init.attrib['sourceURL']
                    
        elif "SegmentList" in tag_name:
            if 'duration' in segment_info.attrib:
                seg_duration = float(segment_info.attrib['duration'])
                
                for segment_URL in segment_info:
                    if "SegmentURL" in get_tag_name(segment_URL.tag or ''):
                        media = segment_URL.attrib.get('media', '')
                        if not media:
                            continue
                            
                        try:
                            size_str = media.split('/')[0].split('_')[-1].split('kbit')[0]
                            segment_size = float(size_str) * SIZE_DICT.get("Kbits", 1024)
                            segurl = cut_url + media
                            
                            URL_LIST.append(segurl)
                            media_object.segment_sizes.append(segment_size)
                        except (IndexError, ValueError) as e:
                            config_dash.LOG.error(f"Error processing segment URL: {e}")
                            continue
                            
                return seg_duration
                
        return None
    except Exception as e:
        config_dash.LOG.error(f"Error in process_segment_info: {e}")
        return None

def get_base_url(mpd_url):
    """
    Extract the base URL path from the MPD URL
    Example: 
    Input: https://dash.akamaized.net/akamai/bbb_30fps/bbb_30fps.mpd
    Output: https://dash.akamaized.net/akamai/bbb_30fps/
    """
    try:
        return mpd_url.rsplit('/', 1)[0] + '/'
    except Exception as e:
        config_dash.LOG.error(f"Error extracting base URL: {e}")
        return ''

def get_url_list(media_object, segment_duration, playback_duration, bitrate):
    """
    Create the URL list for the segments using MPD template
    """
    try:
        # Map bitrates to their representation IDs
        BITRATE_TO_ID = {
            3134488: "bbb_30fps_1024x576_2500k",
            4952892: "bbb_30fps_1280x720_4000k", 
            9914554: "bbb_30fps_1920x1080_8000k",
            254320: "bbb_30fps_320x180_200k",
            507246: "bbb_30fps_320x180_400k",
            759798: "bbb_30fps_480x270_600k",
            1254758: "bbb_30fps_640x360_1000k",
            1013310: "bbb_30fps_640x360_800k",
            1883700: "bbb_30fps_768x432_1500k",
            14931538: "bbb_30fps_3840x2160_12000k"
        }

        if not hasattr(media_object, 'base_url_path'):
            config_dash.LOG.error("No base URL path set in media object")
            return media_object

        representation_id = BITRATE_TO_ID.get(bitrate)
        if not representation_id:
            config_dash.LOG.error(f"No representation ID found for bitrate {bitrate}")
            return media_object

        # Set initialization segment URL using template pattern
        init_template = "$RepresentationID$/$RepresentationID$_0.m4v"
        media_template = "$RepresentationID$/$RepresentationID$_$Number$.m4v"

        # Replace template variables for initialization
        media_object.initialization = init_template.replace("$RepresentationID$", representation_id)
        media_object.initialization = media_object.base_url_path + media_object.initialization
        config_dash.LOG.info(f"Set initialization URL: {media_object.initialization}")

        if not segment_duration or not playback_duration:
            config_dash.LOG.error("Missing duration information")
            return media_object

        # Calculate number of segments
        num_segments = int(playback_duration / segment_duration)
        if num_segments <= 0:
            num_segments = 30
            config_dash.LOG.warning(f"Invalid segment calculation, using default: {num_segments}")

        # Generate segment URLs
        media_object.url_list = []
        for i in range(media_object.start, media_object.start + num_segments):
            segment_url = media_template.replace("$RepresentationID$", representation_id)
            segment_url = segment_url.replace("$Number$", str(i))
            full_url = media_object.base_url_path + segment_url
            media_object.url_list.append(full_url)

        if media_object.url_list:
            config_dash.LOG.info(f"Generated {len(media_object.url_list)} URLs for bitrate {bitrate}")
            config_dash.LOG.debug(f"Sample URL: {media_object.url_list[0]}")
        else:
            config_dash.LOG.error("No URLs generated")

        return media_object

    except Exception as e:
        config_dash.LOG.error(f"Error generating URL list: {str(e)}")
        return media_object



def get_segment_duration(element: ET.Element, ns: dict) -> Optional[float]:
    """
    Extract segment duration from an XML element, checking multiple possible locations
    """
    try:
        # Check SegmentTemplate
        template = element.find('.//SegmentTemplate') if not ns else element.find('.//dash:SegmentTemplate', ns)
        if template is not None:
            duration = template.get('duration')
            timescale = float(template.get('timescale', '1'))
            if duration:
                return float(duration) / timescale

        # Check SegmentList
        seg_list = element.find('.//SegmentList') if not ns else element.find('.//dash:SegmentList', ns)
        if seg_list is not None:
            duration = seg_list.get('duration')
            if duration:
                return float(duration)

        # Check SegmentBase
        seg_base = element.find('.//SegmentBase') if not ns else element.find('.//dash:SegmentBase', ns)
        if seg_base is not None:
            duration = seg_base.get('duration')
            if duration:
                return float(duration)

        return None
    except Exception as e:
        config_dash.LOG.error(f"Error getting segment duration: {str(e)}")
        return None

def read_mpd(mpd_file: str, dashplayback, mpd_url: str) -> Tuple[Optional[object], Optional[float]]:
    """
    Read and parse the MPD file with proper namespace handling
    
    Args:
        mpd_file: Path to the MPD file
        dashplayback: DashPlayback object to populate
        mpd_url: Original MPD URL for base path extraction
    
    Returns:
        Tuple of (DashPlayback object, segment_duration)
    """
    try:
        # Register the DASH namespace
        ET.register_namespace('dash', 'urn:mpeg:dash:schema:mpd:2011')
        
        # Parse with namespace awareness
        tree = ET.parse(mpd_file)
        root = tree.getroot()

        # Get playback duration from MPD
        duration_str = root.get('mediaPresentationDuration')
        if duration_str:
            dashplayback.playback_duration = get_playback_time(duration_str)
            config_dash.LOG.info(f"Found media presentation duration: {dashplayback.playback_duration}s")
            if 'video_metadata' not in config_dash.JSON_HANDLE:
                config_dash.JSON_HANDLE['video_metadata'] = {}
            config_dash.JSON_HANDLE['video_metadata']['playback_duration'] = dashplayback.playback_duration
        else:
            config_dash.LOG.error("No mediaPresentationDuration found in MPD")
            return None, None
            
        # Define namespace map
        ns = {'dash': 'urn:mpeg:dash:schema:mpd:2011'}
        
        # Get base URL from MPD URL
        base_url_path = mpd_url.rsplit('/', 1)[0] + '/'
        config_dash.LOG.info(f"Using base URL path: {base_url_path}")
        
        # Initialize video dictionary if needed
        if not hasattr(dashplayback, 'video'):
            dashplayback.video = {}
            
        # Find Period (with namespace)
        period = root.find('.//dash:Period', ns)
        if period is None:
            # Try without namespace as fallback
            period = root.find('.//Period')
            
        if period is None:
            config_dash.LOG.error("No Period found in MPD")
            return None, None
            
        video_segment_duration = None
        
        # Find video adaptation set
        for adaptation_set in period.findall('.//dash:AdaptationSet', ns):
            mime_type = adaptation_set.get('mimeType', '')
            content_type = adaptation_set.get('contentType', '')
            
            if 'video' in mime_type or content_type == 'video':
                # Get segment template
                segment_template = adaptation_set.find('.//dash:SegmentTemplate', ns)
                if segment_template is not None:
                    duration_str = segment_template.get('duration')
                    timescale_str = segment_template.get('timescale', '1')
                    
                    if duration_str:
                        try:
                            duration = float(duration_str)
                            timescale = float(timescale_str)
                            video_segment_duration = duration / timescale
                            config_dash.LOG.info(f"Found segment duration: {video_segment_duration}s")
                        except (ValueError, TypeError) as e:
                            config_dash.LOG.error(f"Error calculating segment duration: {e}")
                
                # Process representations
                for representation in adaptation_set.findall('.//dash:Representation', ns):
                    try:
                        bandwidth = int(representation.get('bandwidth', '0'))
                        if bandwidth == 0:
                            continue
                            
                        if not hasattr(config_dash.JSON_HANDLE, 'video_metadata'):
                            config_dash.JSON_HANDLE['video_metadata'] = {'available_bitrates': []}
                            
                        if bandwidth not in config_dash.JSON_HANDLE["video_metadata"]["available_bitrates"]:
                            config_dash.JSON_HANDLE["video_metadata"]["available_bitrates"].append(bandwidth)
                            
                        # Create MediaObject for this bandwidth
                        dashplayback.video[bandwidth] = MediaObject()
                        media_object = dashplayback.video[bandwidth]
                        
                        # Store base URL path
                        media_object.base_url_path = base_url_path
                        
                        # Set segment template info if available
                        if segment_template is not None:
                            media_object.start = int(segment_template.get('startNumber', '1'))
                            media_object.timescale = float(timescale_str)
                            media_object.segment_duration = video_segment_duration
                            
                            # Get media template
                            media_template = segment_template.get('media', '')
                            if media_template:
                                media_object.base_url = media_template
                                
                            # Get initialization template
                            init_template = segment_template.get('initialization', '')
                            if init_template:
                                media_object.initialization = init_template
                                
                    except (ValueError, TypeError) as e:
                        config_dash.LOG.error(f"Error processing representation: {e}")
                        continue
                        
                break  # Found video adaptation set

        if not dashplayback.video:
            config_dash.LOG.error("No video representations found")
            return None, None
            
        if video_segment_duration is None:
            config_dash.LOG.error("Could not determine segment duration")
            return None, None

        config_dash.LOG.info(f"Successfully parsed MPD with {len(dashplayback.video)} video representations")
        return dashplayback, video_segment_duration

    except Exception as e:
        config_dash.LOG.error(f"Error in read_mpd: {str(e)}")
        return None, None