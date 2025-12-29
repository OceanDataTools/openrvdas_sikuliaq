#!/usr/bin/env python3
"""
Generates an OpenRVDAS logger configuration for Grafana Live streaming based on Coriolix sensor metadata.

This script performs the following steps:
1. Queries the Coriolix API for a specific `sensor_id` to retrieve configuration (ports, regex formats).
2. Queries the API for parameter definitions to determine data types (float, int, string).
3. Parses NMEA-style regex patterns to identify field names and message types.
4. Automatically detects Latitude/Longitude and Direction field pairs (e.g., `latitude`, `latitude_dir`).
5. Generates a YAML configuration file containing:
    - A `UDPReader` listening on the sensor's transmit port.
    - A `RegexTransform` populated with the sensor's parsing patterns.
    - A `ConvertFieldsTransform` configured to cast types and convert Lat/Lon/Dir triplets into signed decimal degrees.
    - A `TextFileWriter` for local logging.
    - A `GrafanaLiveWriter` for streaming data to a Grafana dashboard.

Usage:
    ./generate_grafana_live_stream.py <sensor_id> [--grafana_url URL] [--api_url URL]

Example:
    ./generate_grafana_live_stream.py metsta155030
    ./generate_grafana_live_stream.py metsta155030 --grafana_url http://192.168.1.50:3000
    ./generate_grafana_live_stream.py metsta155030 --api_url http://localhost:8000/api
"""

import argparse
import ast
import datetime
import json
import re
import sys
import urllib.error
import urllib.request
import warnings
import yaml


class QuotedString(str):
    """
    Custom string class to force specific quoting style in YAML output.
    """
    pass


class FlowList(list):
    """
    Custom list class to force flow style (inline list) in YAML output.
    Example: [a, b] instead of
    - a
    - b
    """
    pass


def quoted_string_representer(dumper, data):
    """
    Representer to output QuotedString with single quotes.
    """
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'")


def flow_list_representer(dumper, data):
    """
    Representer to output FlowList in flow style.
    """
    return dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True)


# Register the custom representers with PyYAML
yaml.add_representer(QuotedString, quoted_string_representer)
yaml.add_representer(FlowList, flow_list_representer)


def map_to_python_type(api_type):
    """
    Maps API data types (e.g., 'ubyte', 'double') to Python type names.
    """
    mapping = {
        'ubyte': 'int',
        'byte': 'int',
        'ushort': 'int',
        'uint': 'int',
        'short': 'int',
        'int': 'int',
        'long': 'int',
        'float': 'float',
        'double': 'float',
        'char': 'str',
        'string': 'str',
        'text': 'str',
        'bool': 'bool',
        'boolean': 'bool'
    }
    return mapping.get(api_type.lower(), 'str')


def extract_message_type(regex_str):
    r"""
    Attempts to extract an NMEA-style talker/message ID from the regex.
    Assumes patterns start with ^\W or ^\$ followed by the ID.
    Example: '^\WWIXDR...' -> 'WIXDR'
    """
    match = re.search(r'^\^\\W([A-Z0-9]+)', regex_str)
    if match:
        return match.group(1)

    match = re.search(r'^\^\\\$([A-Z0-9]+)', regex_str)
    if match:
        return match.group(1)

    return 'unknown'


def extract_regex_groups(regex_list):
    """
    Extracts all named capture groups (?P<name>...) from a list of regex strings.
    Returns a set of field names.
    """
    field_names = set()
    for pattern in regex_list:
        # Find all occurrences of (?P<name>
        matches = re.findall(r'\?P<([^>]+)>', pattern)
        field_names.update(matches)
    return field_names


def fetch_api_data(base_url, endpoint, params):
    """
    Helper to fetch JSON data from the Coriolix API.
    Handles HTTP errors and connection errors gracefully.
    """
    # Ensure base_url doesn't have trailing slash for consistency
    base_url = base_url.rstrip('/')

    query_string = urllib.parse.urlencode(params)
    url = f'{base_url}/{endpoint}/?{query_string}'

    try:
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                sys.stderr.write(f"Error: API returned non-200 status {response.status} for URL: {url}\n")
                return None
            return json.loads(response.read().decode('utf-8'))

    except urllib.error.HTTPError as e:
        sys.stderr.write(f"HTTP Error {e.code}: Failed to retrieve data from {url}\n")
        sys.stderr.write(f"Reason: {e.reason}\n")
        return None

    except urllib.error.URLError as e:
        sys.stderr.write(f"Connection Error: Unable to reach API at {url}\n")
        sys.stderr.write(f"Reason: {e.reason}\n")
        sys.stderr.write("Please verify the --api_url parameter and your network connection.\n")
        return None

    except json.JSONDecodeError as e:
        sys.stderr.write(f"Data Error: Failed to decode JSON response from {url}\n")
        sys.stderr.write(f"Error details: {e}\n")
        return None


def generate_grafana_live_stream(sensor_id, grafana_url=None, api_url=None):
    """
    Generates a Grafana Live Writer configuration for OpenRVDAS based on
    Coriolix sensor metadata. Returns a YAML string.
    """
    # Determine configuration values (defaults)
    grafana_host = grafana_url if grafana_url else 'http://localhost:3000'
    api_base_url = api_url if api_url else 'https://coriolix.sikuliaq.alaska.edu/api'

    # 1. Fetch Sensor Info
    sensor_resp = fetch_api_data(api_base_url, 'sensor', {'sensor_id': sensor_id, 'format': 'json'})
    if not sensor_resp:
        return None

    # Handle object vs list response structure
    sensor_list = sensor_resp.get('objects', []) if isinstance(sensor_resp, dict) else sensor_resp
    sensor_info = next((s for s in sensor_list if s.get('sensor_id') == sensor_id), None)

    if not sensor_info:
        sys.stderr.write(f"Error: Sensor ID '{sensor_id}' not found in API response.\n")
        return None

    # Extract Sensor-Level Configs
    transmit_port = sensor_info.get('transmit_port')

    # Extract Regex Patterns
    raw_regex = sensor_info.get('text_regex_format', [])
    if isinstance(raw_regex, str):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', SyntaxWarning)
                pattern_list = ast.literal_eval(raw_regex)
        except (ValueError, SyntaxError):
            pattern_list = [raw_regex]
    else:
        pattern_list = raw_regex

    # Build field_patterns dict (keyed by Message Type) OR list
    field_patterns_dict = {}
    use_dict = True

    if not pattern_list:
        use_dict = False

    for pattern in pattern_list:
        msg_type = extract_message_type(pattern)
        if msg_type == 'unknown':
            use_dict = False
            break
        # Wrap regex in QuotedString to force single quotes in YAML
        field_patterns_dict[msg_type] = QuotedString(pattern)

    if use_dict:
        field_patterns = field_patterns_dict
    else:
        # Wrap list items in QuotedString as well
        field_patterns = [QuotedString(p) for p in pattern_list]

    # 2. Fetch Parameter Info (Data Types)
    param_resp = fetch_api_data(api_base_url, 'parameter', {'sensor_id': sensor_id, 'format': 'json'})
    if not param_resp:
        return None

    param_list = param_resp.get('objects', []) if isinstance(param_resp, dict) else param_resp

    # Build fields and lat_lon_fields
    fields_map = {}
    lat_lon_fields = {}

    # Gather field names from API parameters
    api_param_names = set()
    for obj in param_list:
        name = obj.get('processing_symbol')
        dtype = obj.get('data_type')
        if name and dtype:
            fields_map[name] = map_to_python_type(dtype)
            api_param_names.add(name)

    # Gather field names from Regex patterns (in case API metadata is incomplete)
    regex_field_names = extract_regex_groups(pattern_list)

    # Combine all known fields to check for pairs
    all_known_fields = api_param_names.union(regex_field_names)

    # Detect Lat/Lon pairs (heuristic: name + name_dir)
    # We iterate over the combined list of known fields
    for name in list(all_known_fields):
        if name.endswith('_dir'):
            base_name = name[:-4]  # strip '_dir'
            if base_name in all_known_fields:
                # We found a pair!
                # Store as FlowList [value_field, direction_field] for inline YAML style
                lat_lon_fields[base_name] = FlowList([base_name, name])

                # REMOVE the base_name from the fields_map if it exists.
                # ConvertFieldsTransform handles it in lat_lon_fields logic.
                if base_name in fields_map:
                    del fields_map[base_name]

                # REMOVE the direction field from simple conversions if it exists.
                if name in fields_map:
                    del fields_map[name]

    # Prepare ConvertFieldsTransform kwargs
    convert_kwargs = {
        'delete_source_fields': True,
        'delete_unconverted_fields': True,
        'fields': fields_map
    }
    # Only add lat_lon_fields if we actually found some
    if lat_lon_fields:
        convert_kwargs['lat_lon_fields'] = lat_lon_fields

    # 3. Construct the Final Dictionary structure
    logger_config = {
        'readers': {
            'class': 'UDPReader',
            'kwargs': {
                'port': transmit_port if transmit_port else 'UNKNOWN_PORT'
            }
        },
        'transforms': [
            {
                'class': 'RegexTransform',
                'module': 'local.sikuliaq.coriolix.logger.transforms.regex_transform',
                'kwargs': {
                    'record_format': QuotedString(r'^(?P<data_id>\w+)\s*'
                                                  r'(?P<data_id_orig>[-\w]+)\s*'
                                                  r'(?P<timestamp>[0-9TZ:\-\.]*)\s*'
                                                  r'(?P<field_string>(.|\r|\n)*)'),
                    'return_das_record': True,
                    'field_patterns': field_patterns
                }
            },
            {
                'class': 'ConvertFieldsTransform',
                'module': 'logger.transforms.convert_fields_transform',
                'kwargs': convert_kwargs
            }
        ],
        'writers': [
            {
                'class': 'TextFileWriter',
            },
            {
                'class': 'GrafanaLiveWriter',
                'module': 'logger.writers.grafana_live_writer',
                'kwargs': {
                    'host': grafana_host,
                    'stream_id': f'openrvdas',
                    'token_file': '/opt/openrvdas/grafana_token.txt'
                }
            }
        ]
    }

    # 4. Generate Output with Header
    yaml_content = yaml.dump(logger_config, sort_keys=False, default_flow_style=False)

    cmd_line = " ".join(sys.argv)
    # Use UTC timezone for the timestamp
    date_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    header = f"""# Logger config for parsing records from {sensor_id} on UDP port {transmit_port}
# and sending them to Grafana Live at {grafana_host}
#
# Generated by: {cmd_line}
# API Source: {api_base_url}
# Date: {date_str}

"""

    return header + yaml_content


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generates an OpenRVDAS logger configuration for Grafana Live streaming."
    )

    # Positional argument
    parser.add_argument(
        'sensor_id',
        help="The Coriolix Sensor ID (e.g., metsta155030)"
    )

    # Optional named arguments
    parser.add_argument(
        '--grafana_url',
        help="Full URL for Grafana Live (default: http://localhost:3000)",
        default=None
    )
    parser.add_argument(
        '--api_url',
        help="Base URL for Coriolix API (default: https://coriolix.sikuliaq.alaska.edu/api)",
        default=None
    )

    args = parser.parse_args()

    # Pass the arguments to the generator
    yaml_output = generate_grafana_live_stream(
        args.sensor_id,
        grafana_url=args.grafana_url,
        api_url=args.api_url
    )

    if yaml_output:
        print(yaml_output)
