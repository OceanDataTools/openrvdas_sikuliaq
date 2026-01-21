#!/usr/bin/env python3
"""
Generates a full OpenRVDAS cruise configuration file for Grafana Live streaming.

This script imports the shared CoriolixSensorConfig to find sensors and
SensorIDMapper to resolve data_id mismatches via UDP inspection or a YAML map.

Usage:
    # Use a saved mapping file (FAST)
    ./generate_grafana_cruise.py --cruise_id NBP1406 --all_sensors --mapping_file sensor_map.yaml

    # Auto-generate mapping by scanning UDP ports (SLOWER)
    ./generate_grafana_cruise.py --cruise_id NBP1406 --all_sensors
"""

import argparse
import datetime
import sys
import os
import yaml

# Import the shared logic
# Note: Ensure these files are in the same directory
try:
    from generate_grafana_live_stream import CoriolixSensorConfig, QuotedString, FlowList
except ImportError:
    sys.exit("Error: Could not import 'CoriolixSensorConfig' from 'generate_grafana_live_stream.py'.")

try:
    from generate_id_mapping import SensorIDMapper
except ImportError:
    sys.exit("Error: Could not import 'SensorIDMapper' from 'generate_id_mapping.py'.")

# Register helpers so the cruise generator outputs correct YAML
yaml.add_representer(QuotedString,
                     lambda dumper, data: dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'"))
yaml.add_representer(FlowList,
                     lambda dumper, data: dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True))


class GrafanaCruiseGenerator:
    def __init__(self, cruise_id, api_url, grafana_url, token_file, mapping_file=None):
        self.cruise_id = cruise_id
        self.grafana_url = grafana_url or 'http://localhost:3000'
        self.token_file = token_file or '/opt/openrvdas/grafana_token.txt'

        # Use the imported class for heavy lifting
        self.sensor_config_gen = CoriolixSensorConfig(api_url=api_url)

        # Initialize Mapping
        self.id_mapping = self._load_id_mapping(mapping_file, api_url)

    def _load_id_mapping(self, mapping_file, api_url):
        """
        Loads ID mapping from a file OR generates it on the fly by scanning UDP.
        Returns a dict: { 'api_sensor_id': 'udp_data_id' }
        """
        if mapping_file:
            if os.path.exists(mapping_file):
                sys.stderr.write(f"Loading sensor ID mapping from {mapping_file}...\n")
                try:
                    with open(mapping_file, 'r') as f:
                        return yaml.safe_load(f) or {}
                except Exception as e:
                    sys.stderr.write(f"Error reading mapping file: {e}\n")
                    return {}
            else:
                sys.stderr.write(f"Warning: Mapping file {mapping_file} not found. Proceeding without mapping.\n")
                return {}
        else:
            sys.stderr.write("No mapping file provided. Scanning network for active Data IDs...\n")
            # Generate on the fly
            mapper = SensorIDMapper(api_url=api_url)
            return mapper.build_mapping()

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
        for sensor_input in sensor_ids:
            # Delegate metadata fetching to the shared class
            # This returns the dict with 'sensor_id' set to the best guess from API (likely hardware ID if slug missing)
            vars_dict = self.sensor_config_gen.get_sensor_metadata(sensor_input)

            if not vars_dict:
                sys.stderr.write(f"Skipping {sensor_input} due to missing metadata.\n")
                continue

            # Retrieve the ID identified by the API (e.g. gnsspo000045)
            api_id = vars_dict['sensor_id']

            # Apply Mapping: If this API ID maps to a different Data ID (e.g. gnss_cnav), use it.
            # This fixes the mismatch where the dashboard expects 'gnss_cnav' but API gives 'gnsspo000045'.
            if api_id in self.id_mapping:
                final_logger_name = self.id_mapping[api_id]
                sys.stderr.write(f"Processing {api_id} -> Mapped to {final_logger_name}\n")

                # Update the vars_dict so the comment inside the logger config matches
                vars_dict['sensor_id'] = final_logger_name
            else:
                final_logger_name = api_id
                sys.stderr.write(f"Processing {api_id}\n")

            # Add to loggers using the FINAL name
            config['loggers'][final_logger_name] = {
                'logger_template': 'grafana_live_stream_logger',
                'variables': vars_dict
            }

            # Add to modes using the FINAL name
            config['modes']['off'][final_logger_name] = f"{final_logger_name}-off"
            config['modes']['on'][final_logger_name] = f"{final_logger_name}-on"

        return config


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate OpenRVDAS cruise config for Grafana streaming."
    )
    parser.add_argument('--sensors', nargs='*', default=[],
                        help='List of sensor IDs (e.g. metsta155030)')
    parser.add_argument('--all_sensors', action='store_true',
                        help='Retrieve all active sensors from the API')

    parser.add_argument('--cruise_id', required=True,
                        help='Cruise ID (e.g. NBP1406)')
    parser.add_argument('--grafana_url', default='http://localhost:3000',
                        help='Grafana URL')
    parser.add_argument('--api_url', default=None,
                        help='Coriolix API URL')
    parser.add_argument('--token_file', default=None,
                        help='Path to Grafana token file')

    # New Argument for Mapping
    parser.add_argument('--mapping_file', default=None,
                        help='Path to YAML file mapping API IDs to Data IDs. If omitted, performs network scan.')

    args = parser.parse_args()

    if not args.sensors and not args.all_sensors:
        parser.error("No sensors specified. Use --sensors <id> [id ...] or --all_sensors")

    generator = GrafanaCruiseGenerator(
        cruise_id=args.cruise_id,
        api_url=args.api_url,
        grafana_url=args.grafana_url,
        token_file=args.token_file,
        mapping_file=args.mapping_file
    )

    sensor_list = []

    if args.sensors:
        sensor_list.extend(args.sensors)

    if args.all_sensors:
        # Delegate finding active sensors to the shared class
        api_sensors = generator.sensor_config_gen.get_active_sensor_ids()
        sensor_list.extend(api_sensors)

    sensor_list = sorted(list(set(sensor_list)))

    if not sensor_list:
        sys.stderr.write("Error: No valid sensors found to process.\n")
        sys.exit(1)

    config_dict = generator.generate_config(sensor_list)

    cmd_line = " ".join(sys.argv)
    header = (
        "###########################################################\n"
        "# Auto-generated OpenRVDAS Cruise Config\n"
        f"# Command: {cmd_line}\n"
        f"# Cruise: {args.cruise_id}\n"
        f"# Date: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n"
        "###########################################################\n"
    )

    print(header + yaml.dump(config_dict, sort_keys=False, default_flow_style=False))
