#!/usr/bin/env python3
"""
Generates a full OpenRVDAS cruise configuration file for Grafana Live streaming.

This script uses the Coriolix API to fetch metadata for a list of sensors and
constructs a cruise definition that uses logger templates and modes.

Usage:
    # Specific sensors
    ./generate_grafana_cruise.py --sensors metsta155030 seapath330 --cruise_id NBP1406

    # All parsable (active + regex defined) sensors
    ./generate_grafana_cruise.py --all_sensors --cruise_id NBP1406
"""

import argparse
import ast
import datetime
import json
import re
import sys
import urllib.error
import urllib.request
import urllib.parse
import warnings
import yaml


# -----------------------------------------------------------------------------
# YAML Formatting Helpers
# -----------------------------------------------------------------------------
class QuotedString(str):
    """Custom string class to force specific quoting style in YAML output."""
    pass


class FlowList(list):
    """Custom list class to force flow style (inline list) in YAML output."""
    pass


def quoted_string_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'")


def flow_list_representer(dumper, data):
    return dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True)


yaml.add_representer(QuotedString, quoted_string_representer)
yaml.add_representer(FlowList, flow_list_representer)


# -----------------------------------------------------------------------------
# Data Processing Helpers
# -----------------------------------------------------------------------------
def map_to_python_type(api_type):
    """Maps API data types (e.g., 'ubyte', 'double') to Python type names."""
    mapping = {
        'ubyte': 'int', 'byte': 'int', 'ushort': 'int', 'uint': 'int',
        'short': 'int', 'int': 'int', 'long': 'int',
        'float': 'float', 'double': 'float',
        'char': 'str', 'string': 'str', 'text': 'str',
        'bool': 'bool', 'boolean': 'bool'
    }
    return mapping.get(api_type.lower(), 'str')


def extract_message_type(regex_str):
    """Attempts to extract an NMEA-style talker/message ID from the regex."""
    match = re.search(r'^\^\\W([A-Z0-9]+)', regex_str)
    if match:
        return match.group(1)
    match = re.search(r'^\^\\\$([A-Z0-9]+)', regex_str)
    if match:
        return match.group(1)
    return 'unknown'


def extract_regex_groups(regex_list):
    """Extracts all named capture groups (?P<name>...) from regex strings."""
    field_names = set()
    for pattern in regex_list:
        matches = re.findall(r'\?P<([^>]+)>', pattern)
        field_names.update(matches)
    return field_names


def fetch_api_data(base_url, endpoint, params):
    """Helper to fetch JSON data from the Coriolix API."""
    base_url = base_url.rstrip('/')
    query_string = urllib.parse.urlencode(params)
    url = f'{base_url}/{endpoint}/?{query_string}'

    try:
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                sys.stderr.write(f"Error: API status {response.status} for {url}\n")
                return None
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        sys.stderr.write(f"API Error fetching {url}: {e}\n")
        return None


# -----------------------------------------------------------------------------
# Generator Class
# -----------------------------------------------------------------------------
class GrafanaCruiseGenerator:
    def __init__(self, cruise_id, api_url, grafana_url, token_file):
        self.cruise_id = cruise_id
        self.api_url = api_url or 'https://coriolix.sikuliaq.alaska.edu/api'
        self.grafana_url = grafana_url or 'http://localhost:3000'
        self.token_file = token_file or '/opt/openrvdas/grafana_token.txt'

    def get_active_sensors(self):
        """Fetches list of all active sensors with UDP ports and Regex definitions."""
        params = {
            'transmit_port__gt': '0',
            'format': 'json',
            'limit': '0'
        }
        sys.stderr.write(f"Fetching sensors from API: {self.api_url}/sensor/ ...\n")

        resp = fetch_api_data(self.api_url, 'sensor', params)
        if not resp:
            sys.stderr.write("Failed to retrieve sensor list.\n")
            return []

        objects = resp.get('objects', []) if isinstance(resp, dict) else resp

        if not isinstance(objects, list):
            sys.stderr.write(f"Unexpected API response type: {type(objects)}\n")
            return []

        active_sensors = []
        for s in objects:
            if not isinstance(s, dict):
                continue

            # Uncomment the following lines to debug available fields:
            # if s.get('sensor_id') == 'metsta155030':
            #     sys.stderr.write(json.dumps(s, indent=2) + "\n")

            # Helper to safely check boolean fields
            def is_true(val):
                if isinstance(val, bool): return val
                if isinstance(val, str): return val.lower() == 'true'
                return False

            # FILTER LOGIC:
            # 1. Must be enabled
            if not is_true(s.get('enabled')):
                continue

            # 2. Must have a regex format defined (implies it's a parsable datagram)
            regex = s.get('text_regex_format')
            if not regex:
                # Sensor might be enabled/active, but without a regex, we can't parse it
                # for the Datagrams page OR for Grafana.
                continue

            if s.get('sensor_id'):
                active_sensors.append(s.get('sensor_id'))

        sys.stderr.write(f"Found {len(active_sensors)} active sensors with parsing rules.\n")
        return active_sensors

    def get_sensor_metadata(self, sensor_id):
        """
        Fetches and processes metadata for a single sensor.
        Returns a dict of variables suitable for the logger definition.
        """
        # 1. Fetch Sensor Info
        sensor_resp = fetch_api_data(self.api_url, 'sensor',
                                     {'sensor_id': sensor_id, 'format': 'json'})
        if not sensor_resp:
            return None

        # Handle object vs list response structure
        sensor_list = (sensor_resp.get('objects', [])
                       if isinstance(sensor_resp, dict) else sensor_resp)
        sensor_info = next((s for s in sensor_list
                            if s.get('sensor_id') == sensor_id), None)

        if not sensor_info:
            sys.stderr.write(f"Warning: Sensor ID '{sensor_id}' not found.\n")
            return None

        transmit_port = sensor_info.get('transmit_port')

        # Check for missing or zero port
        if not transmit_port:
            sys.stderr.write(f"Warning: Sensor '{sensor_id}' has no UDP port defined. Skipping.\n")
            return None

        # Extract Regex Patterns
        raw_regex = sensor_info.get('text_regex_format', [])

        # Double check regex existence here as well
        if not raw_regex:
            sys.stderr.write(f"Warning: Sensor '{sensor_id}' has no regex format. Skipping.\n")
            return None

        if isinstance(raw_regex, str):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', SyntaxWarning)
                    pattern_list = ast.literal_eval(raw_regex)
            except (ValueError, SyntaxError):
                pattern_list = [raw_regex]
        else:
            pattern_list = raw_regex

        # Build field_patterns
        field_patterns_dict = {}
        use_dict = True
        if not pattern_list:
            use_dict = False

        for pattern in pattern_list:
            msg_type = extract_message_type(pattern)
            if msg_type == 'unknown':
                use_dict = False
                break
            field_patterns_dict[msg_type] = QuotedString(pattern)

        if use_dict:
            field_patterns = field_patterns_dict
        else:
            field_patterns = [QuotedString(p) for p in pattern_list]

        # 2. Fetch Parameter Info
        param_resp = fetch_api_data(self.api_url, 'parameter',
                                    {'sensor_id': sensor_id, 'format': 'json'})
        param_list = (param_resp.get('objects', [])
                      if param_resp and isinstance(param_resp, dict)
                      else (param_resp or []))

        fields_map = {}
        lat_lon_fields = {}
        api_param_names = set()

        for obj in param_list:
            name = obj.get('processing_symbol')
            dtype = obj.get('data_type')
            if name and dtype:
                fields_map[name] = map_to_python_type(dtype)
                api_param_names.add(name)

        regex_field_names = extract_regex_groups(pattern_list)
        all_known_fields = api_param_names.union(regex_field_names)

        # Detect Lat/Lon pairs
        for name in list(all_known_fields):
            if name.endswith('_dir'):
                base_name = name[:-4]
                if base_name in all_known_fields:
                    lat_lon_fields[base_name] = FlowList([base_name, name])
                    if base_name in fields_map:
                        del fields_map[base_name]
                    if name in fields_map:
                        del fields_map[name]

        # Construct Transform kwarg dictionaries
        regex_kwargs = {
            'record_format': QuotedString(r'^(?P<data_id>\w+)\s*'
                                          r'(?P<data_id_orig>[-\w]+)\s*'
                                          r'(?P<timestamp>[0-9TZ:\-\.]*)\s*'
                                          r'(?P<field_string>(.|\r|\n)*)'),
            'return_das_record': True,
            'field_patterns': field_patterns
        }

        convert_kwargs = {
            'delete_source_fields': True,
            'delete_unconverted_fields': True,
            'fields': fields_map
        }
        if lat_lon_fields:
            convert_kwargs['lat_lon_fields'] = lat_lon_fields

        return {
            'sensor_id': sensor_id,
            'reader_udp_port': transmit_port,
            'regex_transform_kwargs': regex_kwargs,
            'convert_fields_transform_kwargs': convert_kwargs
        }

    def generate_config(self, sensor_ids):
        """Generates the full cruise YAML dictionary."""

        # 1. Base Structure
        config = {
            'cruise': {
                'id': self.cruise_id,
                'start': '2025-01-01',  # Placeholder
                'end': '2025-12-31'  # Placeholder
            },
            'variables': {
                'grafana_host': self.grafana_url,
                'grafana_token_file': self.token_file,
                'cruise_id': self.cruise_id,
                'log_root': '/var/tmp/log'
            },
            'logger_templates': {
                'grafana_live_stream_logger': {
                    'configs': {
                        'off': {},
                        'on': {
                            'readers': [{
                                'class': 'UDPReader',
                                'kwargs': {'port': '<<reader_udp_port>>'}
                            }],
                            'transforms': [
                                {
                                    'class': 'RegexTransform',
                                    'module': 'local.sikuliaq.coriolix.logger.transforms.regex_transform',
                                    'kwargs': '<<regex_transform_kwargs>>'
                                },
                                {
                                    'class': 'ConvertFieldsTransform',
                                    'module': 'logger.transforms.convert_fields_transform',
                                    'kwargs': '<<convert_fields_transform_kwargs>>'
                                }
                            ],
                            'writers': [
                                {
                                    'class': 'GrafanaLiveWriter',
                                    'module': 'logger.writers.grafana_live_writer',
                                    'kwargs': {
                                        'host': '<<grafana_host>>',
                                        'stream_id': 'openrvdas',
                                        'token_file': '<<grafana_token_file>>'
                                    }
                                }
                            ]
                        }
                    }
                }
            },
            'loggers': {},
            'modes': {
                'off': {},
                'on': {}
            },
            'default_mode': 'off'
        }

        # 2. Process Sensors
        for sensor in sensor_ids:
            sys.stderr.write(f"Processing sensor: {sensor}\n")
            vars_dict = self.get_sensor_metadata(sensor)

            if not vars_dict:
                sys.stderr.write(f"Skipping {sensor} due to missing metadata.\n")
                continue

            # Add to loggers
            config['loggers'][sensor] = {
                'logger_template': 'grafana_live_stream_logger',
                'variables': vars_dict
            }

            # Add to modes
            config['modes']['off'][sensor] = f"{sensor}-off"
            config['modes']['on'][sensor] = f"{sensor}-on"

        return config


# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate OpenRVDAS cruise config for Grafana streaming."
    )
    # Changed to optional nargs='*' to allow --all_sensors to run without specific sensors
    parser.add_argument('--sensors', nargs='*', default=[],
                        help='List of sensor IDs (e.g. metsta155030)')
    parser.add_argument('--all_sensors', action='store_true',
                        help='Retrieve all active sensors with UDP ports and regex rules from the API')

    parser.add_argument('--cruise_id', required=True,
                        help='Cruise ID (e.g. NBP1406)')
    parser.add_argument('--grafana_url', default='http://localhost:3000',
                        help='Grafana URL')
    parser.add_argument('--api_url', default=None,
                        help='Coriolix API URL')
    parser.add_argument('--token_file', default=None,
                        help='Path to Grafana token file')

    args = parser.parse_args()

    # Validate that at least one sensor selection method was provided
    if not args.sensors and not args.all_sensors:
        parser.error("No sensors specified. Use --sensors <id> [id ...] or --all_sensors")

    generator = GrafanaCruiseGenerator(
        cruise_id=args.cruise_id,
        api_url=args.api_url,
        grafana_url=args.grafana_url,
        token_file=args.token_file
    )

    # Compile list of unique sensors
    sensor_list = []

    # 1. Add specific sensors from CLI
    if args.sensors:
        sensor_list.extend(args.sensors)

    # 2. Add all sensors from API if requested
    if args.all_sensors:
        api_sensors = generator.get_active_sensors()
        sensor_list.extend(api_sensors)

    # 3. Deduplicate and sort
    sensor_list = sorted(list(set(sensor_list)))

    if not sensor_list:
        sys.stderr.write("Error: No valid sensors found to process.\n")
        sys.exit(1)

    config_dict = generator.generate_config(sensor_list)

    # Capture command line args
    cmd_line = " ".join(sys.argv)

    # Add header
    header = (
        "###########################################################\n"
        "# Auto-generated OpenRVDAS Cruise Config\n"
        f"# Command: {cmd_line}\n"
        f"# Cruise: {args.cruise_id}\n"
        f"# Date: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n"
        "###########################################################\n"
    )

    print(header + yaml.dump(config_dict, sort_keys=False, default_flow_style=False))
