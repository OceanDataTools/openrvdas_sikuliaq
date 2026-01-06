#!/usr/bin/env python3
"""
Generates an OpenRVDAS logger configuration for Grafana Live streaming based on Coriolix sensor metadata.
Also provides the CoriolixSensorConfig class for use by other scripts.

Usage:
    ./generate_grafana_live_stream.py <sensor_id_or_slug> [--grafana_url URL] [--api_url URL]
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
    Encapsulates logic for retrieving sensor metadata from Coriolix API.
    Performs ALL filtering client-side to ensure reliability.
    """

    def __init__(self, api_url=None):
        self.api_url = api_url or 'https://coriolix.sikuliaq.alaska.edu/api'
        self._sensor_cache = None

    def _fetch_all_sensors(self):
        """
        Fetches the complete list of sensors from the API (limit=0).
        Caches the result to avoid repeated calls during bulk generation.
        """
        if self._sensor_cache is not None:
            return self._sensor_cache

        base_url = self.api_url.rstrip('/')
        # Fetch everything. We do not trust server-side filters.
        url = f'{base_url}/sensor/?limit=0&format=json'

        try:
            with urllib.request.urlopen(url) as response:
                if response.status != 200:
                    sys.stderr.write(f"Error: API status {response.status} for {url}\n")
                    return []
                data = json.loads(response.read().decode('utf-8'))
                # Handle both 'objects' list or direct list
                self._sensor_cache = data.get('objects', []) if isinstance(data, dict) else data
                return self._sensor_cache
        except Exception as e:
            sys.stderr.write(f"API Error fetching {url}: {e}\n")
            return []

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
        r"""
        Attempts to extract an NMEA-style talker/message ID from the regex.
        Looks for patterns like \WID, \$ID, or !ID, ignoring start anchors.
        """
        # 1. Look for literal \W followed by ID (e.g. \WMGHDT, \WPSXN)
        # Matches literal backslash, then W, then the ID.
        match = re.search(r'\\W([A-Z0-9]+)', regex_str)
        if match: return match.group(1)

        # 2. Look for literal \$ followed by ID (e.g. \$GPGGA)
        match = re.search(r'\\\$([A-Z0-9]+)', regex_str)
        if match: return match.group(1)

        # 3. Look for literal ! followed by ID (e.g. !AIVDO for AIS)
        match = re.search(r'!([A-Z0-9]+)', regex_str)
        if match: return match.group(1)

        # 4. Fallback: If it starts with ^ID (rare, but possible for simple formats)
        match = re.search(r'^\^([A-Z0-9]+)', regex_str)
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
        Returns a list of 'slugs' for all active sensors.
        Performs manual filtering for Enabled + Port > 0 + Regex exists.
        """
        all_sensors = self._fetch_all_sensors()
        active_slugs = []

        sys.stderr.write(f"Scanning {len(all_sensors)} sensors from API...\n")

        for s in all_sensors:
            if not isinstance(s, dict): continue

            # 1. Check Enabled (Robust boolean check)
            val = s.get('enabled')
            is_enabled = str(val).lower() == 'true' if isinstance(val, (str, bool)) else False
            if not is_enabled:
                continue

            # 2. Check Port
            if not s.get('transmit_port'):
                continue

            # 3. Check Regex
            if not s.get('text_regex_format'):
                continue

            # 4. Determine Data ID (Slug)
            # We prioritize slug, then short_name, then sensor_id
            slug = s.get('slug') or s.get('short_name') or s.get('sensor_id')
            if slug:
                active_slugs.append(slug)

        sys.stderr.write(f"Found {len(active_slugs)} active sensors.\n")
        return sorted(active_slugs)

    def get_sensor_metadata(self, identifier):
        """
        Finds a sensor by matching the identifier against slug, short_name, or sensor_id.
        Returns the metadata dict using the SLUG as the 'sensor_id' key.
        """
        all_sensors = self._fetch_all_sensors()
        sensor_info = None

        # Manual Lookup Strategy
        # 1. Exact match on slug
        for s in all_sensors:
            if s.get('slug') == identifier:
                sensor_info = s
                break

        # 2. Exact match on short_name
        if not sensor_info:
            for s in all_sensors:
                if s.get('short_name') == identifier:
                    sensor_info = s
                    break

        # 3. Exact match on hardware sensor_id
        if not sensor_info:
            for s in all_sensors:
                if s.get('sensor_id') == identifier:
                    sensor_info = s
                    break

        if not sensor_info:
            sys.stderr.write(f"Warning: Sensor '{identifier}' not found in API list.\n")
            return None

        # --- Extract Configuration ---

        # The 'data_id' we want to use in the config is the slug
        data_id = sensor_info.get('slug') or sensor_info.get('short_name') or sensor_info.get('sensor_id')

        # The hardware ID is needed for the secondary parameter lookup
        hardware_id = sensor_info.get('sensor_id')

        transmit_port = sensor_info.get('transmit_port')
        raw_regex = sensor_info.get('text_regex_format', [])

        if not transmit_port:
            sys.stderr.write(f"Warning: Sensor '{data_id}' has no UDP port.\n")
            return None
        if not raw_regex:
            sys.stderr.write(f"Warning: Sensor '{data_id}' has no regex format.\n")
            return None

        # Process Regex
        if isinstance(raw_regex, str):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', SyntaxWarning)
                    pattern_list = ast.literal_eval(raw_regex)
            except (ValueError, SyntaxError):
                pattern_list = [raw_regex]
        else:
            pattern_list = raw_regex

        field_patterns = {}
        use_dict = True
        for pattern in pattern_list:
            msg_type = self._extract_message_type(pattern)
            if msg_type == 'unknown':
                use_dict = False
                break
            field_patterns[msg_type] = QuotedString(pattern)

        if not use_dict:
            field_patterns = [QuotedString(p) for p in pattern_list]

        # --- Configure Regex Transform Arguments ---
        regex_transform_kwargs = {
            'return_das_record': True,
            'field_patterns': field_patterns,
            'default_data_id': data_id  # Always pass the sensor ID as fallback
        }

        # --- Fetch Parameters (Must use hardware ID) ---
        # We can't reuse _fetch_all_sensors for parameters as that endpoint is likely too huge.
        # We must query the parameter endpoint specifically.
        base_url = self.api_url.rstrip('/')
        param_url = f'{base_url}/parameter/?sensor_id={hardware_id}&format=json'

        try:
            with urllib.request.urlopen(param_url) as response:
                if response.status == 200:
                    p_data = json.loads(response.read().decode('utf-8'))
                    param_list = p_data.get('objects', []) if isinstance(p_data, dict) else p_data
                else:
                    param_list = []
        except Exception:
            # If parameter fetch fails, we proceed with empty map (strings default)
            param_list = []

        fields_map = {}
        lat_lon_fields = {}
        api_param_names = set()

        for obj in param_list:
            name = obj.get('processing_symbol')
            dtype = obj.get('data_type')
            if name and dtype:
                # Build dict for ConvertFieldsTransform including meta
                # Only including data_type for now.
                field_config = {'data_type': self._map_to_python_type(dtype)}
                fields_map[name] = field_config
                api_param_names.add(name)

        regex_field_names = self._extract_regex_groups(pattern_list)
        all_known_fields = api_param_names.union(regex_field_names)

        # Detect Lat/Lon pairs
        for name in list(all_known_fields):
            if name.endswith('_dir'):
                base_name = name[:-4]
                if base_name in all_known_fields:
                    lat_lon_fields[base_name] = FlowList([base_name, name])

        return {
            'sensor_id': data_id,  # This is the Slug/Data ID
            'reader_udp_port': transmit_port,
            'regex_transform_kwargs': regex_transform_kwargs,
            'convert_fields_transform_kwargs': {
                'delete_source_fields': True,
                'delete_unconverted_fields': True,
                'fields': fields_map,
                **({'lat_lon_fields': lat_lon_fields} if lat_lon_fields else {})
            }
        }


# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generates an OpenRVDAS logger configuration for Grafana Live streaming."
    )
    parser.add_argument('sensor_id', help="The Coriolix Sensor Slug or ID (e.g., gnss_cnav)")
    parser.add_argument('--grafana_url', default='http://localhost:3000', help="Full URL for Grafana Live")
    parser.add_argument('--api_url', default=None, help="Base URL for Coriolix API")

    args = parser.parse_args()

    config_gen = CoriolixSensorConfig(api_url=args.api_url)
    meta = config_gen.get_sensor_metadata(args.sensor_id)

    if meta:
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

        cmd_line = " ".join(sys.argv)
        date_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
        # Note: Using the resolved 'sensor_id' (slug) in the comments too
        header = f"""# Logger config for {meta['sensor_id']}
# Generated by: {cmd_line}
# Date: {date_str}

"""
        print(header + yaml.dump(logger_config, sort_keys=False, default_flow_style=False))
