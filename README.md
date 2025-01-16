# AStream: Enhanced DASH Client with PEP Support

AStream is a Python-based emulated video player designed to evaluate the performance of DASH (Dynamic Adaptive Streaming over HTTP) bitrate adaptation algorithms. It now includes support for Performance Enhancing Proxy (PEP) to optimize video streaming performance.

## Features

### Rate Adaptation Algorithms
1. **Basic Adaptation (BASIC)**: A simple throughput-based adaptation
2. **Segment Aware Rate Adaptation (SARA)**: An advanced algorithm considering segment sizes
3. **Netflix Buffer-Based Rate Adaptation**: Implementation based on Netflix's algorithm [1]

### Performance Enhancement
- **PEP Support**: Integrated Performance Enhancing Proxy for improved streaming
- **TCP Optimizations**: Advanced TCP configurations for better throughput
- **Configurable Buffer Sizes**: Adjustable TCP buffer sizes for performance tuning

## Requirements
- Python 3.6+
- Required packages:
  ```
  pip install urllib3 logging threading ssl
  ```

## Installation
```bash
git clone <repository-url>
cd AStream
```

## Usage

### Basic Usage
```bash
python ./dist/client/dash_client.py -m <MPD_URL> -p <PLAYBACK_TYPE>
```

### Running the PEP Proxy
```bash
python ./dist/proxy/proxy.py --listen-host localhost --listen-port 8888 --target-host dash.akamaized.net --target-port 443
```

### Running Client with PEP Support
```bash
python ./dist/client/dash_client.py -m <MPD_URL> -p <PLAYBACK_TYPE> --use-pep --pep-host localhost --pep-port 8888
```

Note: Make sure to start the proxy server before running the client with PEP support.

### Command Line Options
```
dash_client.py [-h] [-m MPD] [-l] [-p PLAYBACK] [-n SEGMENT_LIMIT] [-d] 
               [--use-pep] [--pep-host PEP_HOST] [--pep-port PEP_PORT] 
               [--buffer-size BUFFER_SIZE]

optional arguments:
  -h, --help            show help message and exit
  -m MPD, --MPD MPD     URL to the MPD File
  -l, --LIST            List all representations and quit
  -p PLAYBACK           Playback type ('basic', 'sara', 'netflix', or 'all')
  -n SEGMENT_LIMIT      Segment number limit
  -d, --DOWNLOAD        Keep video files after playback
  --use-pep            Enable PEP support
  --pep-host           PEP proxy host (default: localhost)
  --pep-port           PEP proxy port (default: 8888)
  --buffer-size        TCP buffer size in bytes (default: 2MB)
```

## Logging
The system provides comprehensive logging for analysis:

### Buffer Logs
- Epoch time
- Current playback time
- Buffer size (in segments)
- Playback state

### Playback Logs
- Epoch time
- Playback time
- Segment information (number, size, duration)
- Bitrate details
- Download rates
- TCP metrics (when using PEP)

### PEP Logs
- Connection establishment
- TCP optimization parameters
- Transfer rates
- Error conditions

## Architecture

### Components
1. **DASH Client**: Main video player implementation
2. **PEP Proxy**: Performance enhancement layer
3. **Rate Adaptation Modules**: Different adaptation algorithms
4. **Download Manager**: Handles segment retrieval
5. **Buffer Manager**: Manages playback buffer

### Data Flow
```
Client -> PEP Proxy -> CDN
   ↑         ↑          ↓
   |         |          |
   └─────────└──────────┘
   Optimized Data Flow
```

## Performance Optimization Tips
- Enable PEP for high-latency networks
- Adjust buffer size based on available memory
- Configure TCP optimizations for your network

## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## References
[1] Te-Yuan Huang, et al. "A buffer-based approach to rate adaptation: evidence from a large video streaming service." SIGCOMM '14, 2014.

## License
MIT License - see LICENSE file for details