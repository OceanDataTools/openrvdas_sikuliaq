#!/usr/bin/env python3
"""
Generates a full OpenRVDAS cruise configuration file for Grafana Live streaming.

Usage:
    # Specific sensors
    ./generate_grafana_cruise.py --sensors metsta155030 seapath330 --cruise_id NBP1406

    # All parsable (active + regex defined) sensors
    ./generate_grafana_cruise.py --all_sensors --cruise_id NBP1406
"""

import argparse
import datetime
import sys
import yaml

# Import the shared logic
# Note: Ensure both files are in the same directory
try:
    from generate_grafana_live_stream import CoriolixSensorConfig, QuotedString, FlowList
except ImportError:
    sys.exit(
        "Error: Could not import 'CoriolixSensorConfig' from 'generate_grafana_live_stream.py'. Ensure both scripts are in the same directory.")

# Register helpers so the cruise generator outputs correct YAML
yaml.add_representer(QuotedString,
                     lambda dumper, data: dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'"))
yaml.add_representer(FlowList,
                     lambda dumper, data: dumper.represent_sequence('tag:yaml.org,2002:seq', data, flow_style=True))


class GrafanaCruiseGenerator:
    def __init__(self, cruise_id, api_url, grafana_url, token_file):
        self.cruise_id = cruise_id
        self.grafana_url = grafana_url or 'http://localhost:3000'
        self.token_file = token_file or '/opt/openrvdas/grafana_token.txt'

        # Use the imported class for heavy lifting
        self.sensor_config_gen = CoriolixSensorConfig(api_url=api_url)

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

            # Delegate metadata fetching to the shared class
            vars_dict = self.sensor_config_gen.get_sensor_metadata(sensor)

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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate OpenRVDAS cruise config for Grafana streaming."
    )
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

    if not args.sensors and not args.all_sensors:
        parser.error("No sensors specified. Use --sensors <id> [id ...] or --all_sensors")

    generator = GrafanaCruiseGenerator(
        cruise_id=args.cruise_id,
        api_url=args.api_url,
        grafana_url=args.grafana_url,
        token_file=args.token_file
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
