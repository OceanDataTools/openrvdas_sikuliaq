#!/usr/bin/env python3
"""
Generates a Grafana Dashboard JSON model for OpenRVDAS Live data.

This script:
1. Discovers active sensors via the Coriolix API.
2. Resolves their true 'data_id' (e.g. gnss_cnav) using a mapping file or UDP scan.
3. Generates a dashboard with:
   - One ROW per Sensor (data_id).
   - One PANEL per Message Type (Channel) within that row.
   - Handles both 'stream/openrvdas/{data_id}' and 'stream/openrvdas/{data_id}/{msg_type}' conventions.

Usage:
    # Use existing mapping (Fast)
    ./generate_grafana_dashboard.py --title "R/V Sikuliaq Live" --mapping_file sensor_map.yaml > dashboard.json

    # Auto-scan UDP for IDs (Slower)
    ./generate_grafana_dashboard.py --title "R/V Sikuliaq Live" --all_sensors > dashboard.json
"""

import argparse
import json
import sys
import os
import yaml

# Import shared logic
try:
    from generate_grafana_live_stream import CoriolixSensorConfig
except ImportError:
    sys.exit("Error: Could not import 'CoriolixSensorConfig' from 'generate_grafana_live_stream.py'.")

try:
    from generate_id_mapping import SensorIDMapper
except ImportError:
    # Fail gracefully if mapping script isn't there, but warn
    SensorIDMapper = None
    pass


class GrafanaDashboardGenerator:
    def __init__(self, api_url, mapping_file=None):
        self.sensor_config = CoriolixSensorConfig(api_url=api_url)
        self.PANEL_HEIGHT = 6
        self.PANEL_WIDTH = 8  # 3 panels per row (24/8 = 3)
        self.ROW_WIDTH = 24  # Grafana standard grid width

        # Load or Generate Mapping
        self.id_mapping = self._load_id_mapping(mapping_file, api_url)

    def _load_id_mapping(self, mapping_file, api_url):
        """Loads ID mapping from file or generates on the fly."""
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
                sys.stderr.write(f"Warning: Mapping file {mapping_file} not found. Scanning UDP...\n")

        # Fallback to scanning
        if SensorIDMapper:
            sys.stderr.write("Scanning network for active Data IDs...\n")
            mapper = SensorIDMapper(api_url=api_url)
            return mapper.build_mapping()
        else:
            return {}

    def _get_base_dashboard(self, title):
        """Returns the skeleton of a Grafana dashboard."""
        return {
            "title": title,
            "uid": None,
            "timezone": "browser",
            "schemaVersion": 36,
            "refresh": "1s",  # Fast refresh for live feel
            "panels": [],
            "templating": {
                "list": []
            },
            "time": {
                "from": "now-5m",
                "to": "now"
            }
        }

    def _create_row(self, title, y_pos):
        """Creates a collapsible row panel."""
        return {
            "type": "row",
            "title": title,
            "gridPos": {
                "h": 1,
                "w": 24,
                "x": 0,
                "y": y_pos
            },
            "collapsed": False,
            "panels": []
        }

    def _create_channel_panel(self, title, channel, x_pos, y_pos):
        """
        Creates a Stat panel subscribed to a specific channel (message type).
        Includes mapping for 'No Data' -> Red.
        Displays both Value and Field Name.
        """
        panel = {
            "type": "stat",
            "title": title,
            "gridPos": {
                "h": self.PANEL_HEIGHT,
                "w": self.PANEL_WIDTH,
                "x": x_pos,
                "y": y_pos
            },
            "datasource": {
                "type": "datasource",
                "uid": "grafana"
            },
            "targets": [
                {
                    "channel": channel,
                    "datasource": {
                        "type": "datasource",
                        "uid": "grafana"
                    },
                    "queryType": "measurements",
                    "refId": "A"
                }
            ],
            "options": {
                "reduceOptions": {
                    "values": False,
                    "calcs": ["lastNotNull"],
                    "fields": ""
                },
                "orientation": "auto",
                "textMode": "value_and_name",  # <--- Shows field name alongside value
                "colorMode": "background",
                "graphMode": "none",
                "justifyMode": "auto"
            },
            "fieldConfig": {
                "defaults": {
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": None}  # Default Green if value exists
                        ]
                    },
                    "mappings": [
                        {
                            "type": "special",
                            "options": {
                                "match": "null",
                                "result": {
                                    "text": "No Data",
                                    "color": "red",
                                    "index": 0
                                }
                            }
                        }
                    ],
                    "noValue": "No Data",  # Fallback text
                    "color": {"mode": "thresholds"}
                },
                "overrides": []
            }
        }
        return panel

    def generate(self, sensor_ids, dashboard_title):
        dashboard = self._get_base_dashboard(dashboard_title)

        current_y = 0

        for sensor_input in sensor_ids:
            # 1. Fetch Metadata (using API ID)
            meta = self.sensor_config.get_sensor_metadata(sensor_input)
            if not meta:
                continue

            # 2. Resolve the Data ID (Slug)
            api_id = meta['sensor_id']
            data_id = self.id_mapping.get(api_id, api_id)

            # 3. Identify Message Types (Channels)
            regex_kwargs = meta.get('regex_transform_kwargs', {})
            field_patterns = regex_kwargs.get('field_patterns', {})

            message_types = []

            if isinstance(field_patterns, dict):
                message_types = list(field_patterns.keys())
            elif isinstance(field_patterns, list):
                for pattern in field_patterns:
                    extracted = self.sensor_config._extract_message_type(str(pattern))
                    if extracted and extracted != 'unknown':
                        message_types.append(extracted)

            # Remove duplicates and sort
            message_types = sorted(list(set(message_types)))

            # 4. Create Row for the Sensor
            row = self._create_row(f"Sensor: {data_id}", current_y)
            dashboard["panels"].append(row)
            current_y += 1

            # 5. Create Panels
            # Logic: If no specific message types found, assume default stream (no suffix).
            # If message types found, create one panel per type.

            current_x = 0

            if not message_types:
                # Case A: No specific message types -> "stream/openrvdas/{data_id}"
                channel = f"stream/openrvdas/{data_id}"
                panel_title = f"{data_id} (Raw)"

                panel = self._create_channel_panel(panel_title, channel, current_x, current_y)
                dashboard["panels"].append(panel)
                current_y += self.PANEL_HEIGHT  # Only one panel, advance Y immediately

            else:
                # Case B: Specific message types -> "stream/openrvdas/{data_id}/{msg_type}"
                for msg_type in message_types:
                    channel = f"stream/openrvdas/{data_id}/{msg_type}"
                    panel_title = f"{msg_type}"

                    panel = self._create_channel_panel(
                        title=panel_title,
                        channel=channel,
                        x_pos=current_x,
                        y_pos=current_y
                    )

                    dashboard["panels"].append(panel)

                    # Grid Layout Logic
                    current_x += self.PANEL_WIDTH
                    if current_x >= self.ROW_WIDTH:
                        current_x = 0
                        current_y += self.PANEL_HEIGHT

                # Advance Y if row wasn't filled perfectly
                if current_x > 0:
                    current_y += self.PANEL_HEIGHT

        return json.dumps(dashboard, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Grafana Dashboard JSON")
    parser.add_argument('--title', default="OpenRVDAS Real-time", help="Dashboard title")
    parser.add_argument('--api_url', default=None, help="Coriolix API URL")

    parser.add_argument('--sensors', nargs='*', help="List of sensors (API IDs)")
    parser.add_argument('--all_sensors', action='store_true', help="Include all active sensors")

    parser.add_argument('--mapping_file', default=None,
                        help="Path to YAML mapping file (API_ID -> DATA_ID). If omitted, scans UDP.")

    args = parser.parse_args()

    # 1. Initialize Generator
    generator = GrafanaDashboardGenerator(args.api_url, args.mapping_file)

    # 2. Determine List of Sensors to Process
    sensors_to_process = []

    if args.sensors:
        sensors_to_process.extend(args.sensors)

    if args.all_sensors:
        api_sensors = generator.sensor_config.get_active_sensor_ids()
        sensors_to_process.extend(api_sensors)

    sensors_to_process = sorted(list(set(sensors_to_process)))

    if not sensors_to_process:
        sys.stderr.write("No sensors specified. Use --sensors or --all_sensors.\n")
        sys.exit(1)

    # 3. Generate JSON
    print(generator.generate(sensors_to_process, args.title))
