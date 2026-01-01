#!/usr/bin/env python3
"""
Generates an OpenRVDAS logger configuration for Grafana Live streaming based on Coriolix sensor metadata.
Also provides the CoriolixSensorConfig class for use by other scripts.

Usage:
    ./generate_grafana_live_stream.py <sensor_id> [--grafana_url URL] [--api_url URL]
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
# YAML Formatting Helpers (Exported for reuse)
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
# Reusable Configuration Class
# -----------------------------------------------------------------------------
class CoriolixSensorConfig:
    """
    Encapsulates logic for retrieving sensor metadata from Coriolix API
    and generating OpenRVDAS configuration variables.
    """

    def __init__(self, api_url=None):
        self.api_url = api_url or 'https://coriolix.sikuliaq.alaska.edu/api'

    def _fetch_api_data(self, endpoint, params):
        """Helper to fetch JSON data from the Coriolix API."""
        base_url = self.api_url.rstrip('/')
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

    def _map_to_python_type(self, api_type):
        """Maps API data types to Python type names."""
        mapping = {
            'ubyte': 'int', 'byte': 'int', 'ushort': 'int', 'uint': 'int',
            'short': 'int', 'int': 'int', 'long': 'int',
            'float': 'float', 'double': 'float',
            'char': 'str', 'string': 'str', 'text': 'str',
            'bool': 'bool', 'boolean': 'bool'
        }
        return mapping.get(api_type.lower(), 'str')

    def _extract_message_type(self, regex_str):
        """Attempts to extract an NMEA-style talker/message ID from the regex."""
        match = re.search(r'^\^\\W([A-Z0-9]+)', regex_str)
        if match: return match.group(1)
        match = re.search(r'^\^\\\$([A-Z0-9]+)', regex_str)
        if match: return match.group(1)
        return 'unknown'

    def _extract_regex_groups(self, regex_list):
        """Extracts all named capture groups from regex strings."""
        field_names = set()
        for pattern in regex_list:
            matches = re.findall(r'\?P<([^>]+)>', pattern)
            field_names.update(matches)
        return field_names

    def get_active_sensor_ids(self):
        """
        Fetches list of all active sensors that have UDP ports and regex definitions.
        """
        params = {
            'transmit_port__gt': '0',
            'format': 'json',
            'limit': '0'
        }
        sys.stderr.write(f"Fetching sensors from API: {self.api_url}/sensor/ ...\n")

        resp = self._fetch_api_data('sensor', params)
        if not resp:
            return []

        objects = resp.get('objects', []) if isinstance(resp, dict) else resp
        if not isinstance(objects, list):
            return []

        active_sensors = []
        for s in objects:
            if not isinstance(s, dict): continue

            # Helper for loose boolean check
            def is_true(val):
                if isinstance(val, bool): return val
                if isinstance(val, str): return val.lower() == 'true'
                return False

            # Must be enabled
            if not is_true(s.get('enabled')): continue

            # Must have regex
            if not s.get('text_regex_format'): continue

            if s.get('sensor_id'):
                active_sensors.append(s.get('sensor_id'))

        sys.stderr.write(f"Found {len(active_sensors)} active sensors with parsing rules.\n")
        return active_sensors

    def get_sensor_metadata(self, sensor_id):
        """
        Fetches metadata for a single sensor and returns the dictionary
        of variables (kwargs) needed for OpenRVDAS configuration.
        """
        # 1. Fetch Sensor Info
        sensor_resp = self._fetch_api_data('sensor', {'sensor_id': sensor_id, 'format': 'json'})
        if not sensor_resp: return None

        sensor_list = (sensor_resp.get('objects', [])
                       if isinstance(sensor_resp, dict) else sensor_resp)
        sensor_info = next((s for s in sensor_list
                            if s.get('sensor_id') == sensor_id), None)

        if not sensor_info:
            sys.stderr.write(f"Warning: Sensor ID '{sensor_id}' not found.\n")
            return None

        transmit_port = sensor_info.get('transmit_port')
        if not transmit_port:
            sys.stderr.write(f"Warning: Sensor '{sensor_id}' has no UDP port. Skipping.\n")
            return None

        raw_regex = sensor_info.get('text_regex_format', [])
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
        for pattern in pattern_list:
            msg_type = self._extract_message_type(pattern)
            if msg_type == 'unknown':
                use_dict = False
                break
            field_patterns_dict[msg_type] = QuotedString(pattern)

        if use_dict:
            field_patterns = field_patterns_dict
        else:
            field_patterns = [QuotedString(p) for p in pattern_list]

        # 2. Fetch Parameter Info
        param_resp = self._fetch_api_data('parameter', {'sensor_id': sensor_id, 'format': 'json'})
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
                fields_map[name] = self._map_to_python_type(dtype)
                api_param_names.add(name)

        regex_field_names = self._extract_regex_groups(pattern_list)
        all_known_fields = api_param_names.union(regex_field_names)

        # Detect Lat/Lon pairs
        for name in list(all_known_fields):
            if name.endswith('_dir'):
                base_name = name[:-4]
                if base_name in all_known_fields:
                    lat_lon_fields[base_name] = FlowList([base_name, name])
                    if base_name in fields_map: del fields_map[base_name]
                    if name in fields_map: del fields_map[name]

        # Construct kwargs
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


# -----------------------------------------------------------------------------
# Main Execution (Standalone Mode)
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generates an OpenRVDAS logger configuration for Grafana Live streaming."
    )
    parser.add_argument('sensor_id', help="The Coriolix Sensor ID (e.g., metsta155030)")
    parser.add_argument('--grafana_url', default='http://localhost:3000', help="Full URL for Grafana Live")
    parser.add_argument('--api_url', default=None, help="Base URL for Coriolix API")

    args = parser.parse_args()

    # Instantiate the shared class
    config_gen = CoriolixSensorConfig(api_url=args.api_url)

    # Get metadata
    meta = config_gen.get_sensor_metadata(args.sensor_id)

    if meta:
        # Construct specific logger config for this standalone script
        logger_config = {
            'readers': {
                'class': 'UDPReader',
                'kwargs': {'port': meta['reader_udp_port']}
            },
            'transforms': [
                {
                    'class': 'RegexTransform',
                    'module': 'local.sikuliaq.coriolix.logger.transforms.regex_transform',
                    'kwargs': meta['regex_transform_kwargs']
                },
                {
                    'class': 'ConvertFieldsTransform',
                    'module': 'logger.transforms.convert_fields_transform',
                    'kwargs': meta['convert_fields_transform_kwargs']
                }
            ],
            'writers': [
                {'class': 'TextFileWriter'},
                {
                    'class': 'GrafanaLiveWriter',
                    'module': 'logger.writers.grafana_live_writer',
                    'kwargs': {
                        'host': args.grafana_url,
                        'stream_id': 'openrvdas',
                        'token_file': '/opt/openrvdas/grafana_token.txt'
                    }
                }
            ]
        }

        # Header
        cmd_line = " ".join(sys.argv)
        date_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
        header = f"""# Logger config for parsing records from {args.sensor_id} on UDP port {meta['reader_udp_port']}
# and sending them to Grafana Live at {args.grafana_url}
#
# Generated by: {cmd_line}
# API Source: {config_gen.api_url}
# Date: {date_str}

"""
        print(header + yaml.dump(logger_config, sort_keys=False, default_flow_style=False))
