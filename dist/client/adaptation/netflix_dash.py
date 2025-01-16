#!/usr/bin/env python

from __future__ import division
__author__ = 'pjuluri'

"""
 The current module is the buffer based adaptaion scheme used by Netflix. Current design is based
 on the design from the paper:

[1] Huang, Te-Yuan, et al. "A buffer-based approach to rate adaptation: Evidence from a large video streaming service."
    Proceedings of the 2014 ACM conference on SIGCOMM. ACM, 2014.
"""

import config_dash
from collections import OrderedDict


def get_rate_map(bitrates):
    """
    Module to generate the rate map for the bitrates, reservoir, and cushion
    """
    try:
        rate_map = OrderedDict()
        # Set minimum bitrate for reservoir
        rate_map[config_dash.NETFLIX_RESERVOIR] = bitrates[0]
        
        # Handle intermediate levels
        intermediate_levels = bitrates[1:-1] if len(bitrates) > 2 else []
        if intermediate_levels:
            marker_length = (config_dash.NETFLIX_CUSHION - config_dash.NETFLIX_RESERVOIR)/(len(intermediate_levels) + 1)
            current_marker = config_dash.NETFLIX_RESERVOIR + marker_length
            for bitrate in intermediate_levels:
                rate_map[current_marker] = bitrate
                current_marker += marker_length
                
        # Set maximum bitrate for cushion
        rate_map[config_dash.NETFLIX_CUSHION] = bitrates[-1]
        return rate_map
    except Exception as e:
        config_dash.LOG.error(f"Error creating rate map: {e}")
        # Return simple fallback rate map
        return OrderedDict({
            config_dash.NETFLIX_RESERVOIR: bitrates[0],
            config_dash.NETFLIX_CUSHION: bitrates[-1]
        })

def netflix_dash(bitrates, dash_player, segment_download_rate, curr_bitrate, average_segment_sizes, rate_map, state):
    """
    Netflix rate adaptation module with improved error handling
    """
    try:
        # Sort and validate bitrates
        bitrates = sorted([int(b) for b in bitrates])
        if not bitrates:
            return bitrates[0], None, "INITIAL"
            
        # Initialize if needed
        if not (curr_bitrate and rate_map and state):
            rate_map = get_rate_map(bitrates)
            state = "INITIAL"
            return bitrates[0], rate_map, state
            
        available_video_segments = max(0, dash_player.buffer.qsize() - dash_player.initial_buffer)
        
        # Handle INITIAL state
        if state == "INITIAL":
            next_bitrate = curr_bitrate
            
            # Safety check for current bitrate
            if curr_bitrate not in bitrates:
                config_dash.LOG.warning(f"Current bitrate {curr_bitrate} not in available bitrates")
                return bitrates[0], rate_map, state
                
            try:
                # Calculate buffer change
                if segment_download_rate > 0:
                    delta_B = dash_player.segment_duration - average_segment_sizes[curr_bitrate]/segment_download_rate
                else:
                    delta_B = 0
                    
                # Check if we can increase bitrate
                current_index = bitrates.index(curr_bitrate)
                if (delta_B > config_dash.NETFLIX_INITIAL_FACTOR * dash_player.segment_duration and 
                    current_index < len(bitrates) - 1):
                    next_bitrate = bitrates[current_index + 1]
                
                # Check if we should transition to RUNNING state
                if available_video_segments >= config_dash.NETFLIX_INITIAL_BUFFER:
                    rate_map_next_bitrate = get_rate_netflix(
                        bitrates, 
                        available_video_segments,
                        config_dash.NETFLIX_BUFFER_SIZE, 
                        rate_map
                    )
                    
                    if rate_map_next_bitrate and rate_map_next_bitrate > next_bitrate:
                        next_bitrate = rate_map_next_bitrate
                        state = "RUNNING"
                        
            except (ValueError, IndexError) as e:
                config_dash.LOG.error(f"Error in INITIAL state: {e}")
                return curr_bitrate, rate_map, state
                
        # Handle RUNNING state
        else:
            next_bitrate = get_rate_netflix(
                bitrates,
                available_video_segments,
                config_dash.NETFLIX_BUFFER_SIZE,
                rate_map
            )
            if not next_bitrate:
                next_bitrate = curr_bitrate
                
        return next_bitrate, rate_map, state
        
    except Exception as e:
        config_dash.LOG.error(f"Error in netflix_dash: {e}")
        # Return safe fallback values
        return bitrates[0], rate_map, state

def get_rate_netflix(bitrates, current_buffer_occupancy, buffer_size=config_dash.NETFLIX_BUFFER_SIZE, rate_map=None):
    """
    Get next bitrate based on buffer occupancy
    """
    try:
        if not rate_map:
            rate_map = get_rate_map(bitrates)
            
        # Calculate buffer percentage safely
        try:
            buffer_percentage = current_buffer_occupancy/buffer_size if buffer_size else 0
        except ZeroDivisionError:
            config_dash.LOG.error("Buffer size is zero")
            return bitrates[0]
            
        # Select bitrate based on buffer percentage
        if buffer_percentage <= config_dash.NETFLIX_RESERVOIR:
            return bitrates[0]
        elif buffer_percentage >= config_dash.NETFLIX_CUSHION:
            return bitrates[-1]
        else:
            # Find appropriate bitrate from rate map
            for marker in reversed(rate_map.keys()):
                if marker < buffer_percentage:
                    return rate_map[marker]
            
            # Fallback to current or minimum bitrate
            return rate_map.get(min(rate_map.keys()), bitrates[0])
            
    except Exception as e:
        config_dash.LOG.error(f"Error in get_rate_netflix: {e}")
        return bitrates[0]  # Safe fallback to minimum bitrate