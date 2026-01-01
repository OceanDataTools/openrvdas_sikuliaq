#!/usr/bin/env python3
"""
Generates a Grafana Dashboard JSON model for OpenRVDAS Live data.

This script uses the CoriolixSensorConfig class to discover active sensors (by slug),
then constructs a dashboard JSON file.

Usage:
    ./generate_grafana_dashboard.py --title "R/V Sikuliaq Real-time" --all_sensors > dashboard.json
"""

import argparse
import json
import sys
import datetime

# Import shared logic
try:
    from generate_grafana_live_stream import CoriolixSensorConfig
except ImportError:
    sys.exit(
        "Error: Could not import 'CoriolixSensorConfig' from 'generate_grafana_live_stream.py'. Ensure both files are in the same directory.")


class GrafanaDashboardGenerator:
    def __init__(self, api_url):
        self.sensor_config = CoriolixSensorConfig(api_url=api_url)
        self.PANEL_HEIGHT = 8
        self.ROW_WIDTH = 24  # Grafana standard grid width

    def _get_base_dashboard(self, title):
        """Returns the skeleton of a Grafana dashboard."""
        return {
            "title": title,
            "uid": None,
            "timezone": "browser",
            "schemaVersion": 36,
            "refresh": "5s",
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

    def _create_sensor_panel(self, title, targets, y_pos):
        """
        Creates a single Stat panel with multiple targets (channels).
        """
        panel = {
            "type": "stat",
            "title": title,
            "gridPos": {
                "h": self.PANEL_HEIGHT,
                "w": self.ROW_WIDTH,  # Full width
                "x": 0,
                "y": y_pos
            },
            "datasource": {
                "type": "datasource",
                "uid": "grafana"
            },
            "targets": targets,
            "options": {
                "reduceOptions": {
                    "values": False,
                    "calcs": ["lastNotNull"],
                    "fields": ""
                },
                "orientation": "auto",
                "textMode": "auto",
                "colorMode": "background",
                "graphMode": "none",
                "justifyMode": "auto"
            },
            "fieldConfig": {
                "defaults": {
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": None}
                        ]
                    },
                    "mappings": [],
                    "color": {"mode": "thresholds"}
                },
                "overrides": []
            }
        }
        return panel

    def generate(self, sensor_ids, dashboard_title):
        dashboard = self._get_base_dashboard(dashboard_title)

        current_y = 0

        for sensor_id in sensor_ids:
            # 1. Fetch Metadata (using slug/id)
            meta = self.sensor_config.get_sensor_metadata(sensor_id)
            if not meta:
                continue

            # 2. Identify Message Types (Streams)
            regex_kwargs = meta.get('regex_transform_kwargs', {})
            field_patterns = regex_kwargs.get('field_patterns', {})

            message_types = []

            # If it's a dictionary, the keys are the message types (e.g., 'GPGGA', 'WIMDA')
            if isinstance(field_patterns, dict):
                message_types = list(field_patterns.keys())

            # If it's a list, we try to re-extract using the helper logic
            elif isinstance(field_patterns, list):
                for pattern in field_patterns:
                    extracted = self.sensor_config._extract_message_type(str(pattern))
                    if extracted != 'unknown':
                        message_types.append(extracted)

                if not message_types:
                    message_types = ['+']

            # 3. Create Row
            row = self._create_row(f"Sensor: {sensor_id}", current_y)
            dashboard["panels"].append(row)
            current_y += 1

            # 4. Create Targets (one per message type)
            targets = []

            def make_target(channel_name, ref_id):
                return {
                    "channel": channel_name,
                    "datasource": {
                        "type": "datasource",
                        "uid": "grafana"
                    },
                    "queryType": "measurements",
                    "refId": ref_id
                }

            import string
            letters = list(string.ascii_uppercase)

            for i, msg_type in enumerate(message_types):
                # Channel Pattern: stream/openrvdas/{slug}/{message_type}/{message_type}
                if msg_type == '+':
                    channel = f"stream/openrvdas/{sensor_id}/+"
                else:
                    channel = f"stream/openrvdas/{sensor_id}/{msg_type}/{msg_type}"

                ref_id = letters[i % len(letters)]
                targets.append(make_target(channel, ref_id))

            # 5. Create Panel
            panel = self._create_sensor_panel(
                title=f"{sensor_id} Live Data",
                targets=targets,
                y_pos=current_y
            )

            dashboard["panels"].append(panel)
            current_y += self.PANEL_HEIGHT

        return json.dumps(dashboard, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Grafana Dashboard JSON")
    parser.add_argument('--title', default="OpenRVDAS Real-time", help="Dashboard title")
    parser.add_argument('--api_url', default=None, help="Coriolix API URL")
    parser.add_argument('--sensors', nargs='*', help="List of sensors (slugs)")
    parser.add_argument('--all_sensors', action='store_true', help="Include all active sensors")

    args = parser.parse_args()

    generator = GrafanaDashboardGenerator(args.api_url)

    sensors = []
    if args.sensors:
        sensors.extend(args.sensors)
    if args.all_sensors:
        sensors.extend(generator.sensor_config.get_active_sensor_ids())

    sensors = sorted(list(set(sensors)))

    if not sensors:
        sys.stderr.write("No sensors specified.\n")
        sys.exit(1)

    print(generator.generate(sensors, args.title))
