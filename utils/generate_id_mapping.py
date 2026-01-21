#!/usr/bin/env python3
"""
Scans active Coriolix sensors via UDP to discover their true 'data_id'.
Generates a YAML mapping file (API_ID -> DATA_ID) to resolve naming mismatches.

Usage:
    ./generate_id_mapping.py > sensor_map.yaml

    # Or import in other scripts:
    # from generate_id_mapping import SensorIDMapper
    # mapper = SensorIDMapper()
    # mapping = mapper.build_mapping()
"""

import socket
import re
import sys
import yaml
import threading
import logging
from queue import Queue

# Import the shared configuration logic
try:
    from generate_grafana_live_stream import CoriolixSensorConfig
except ImportError:
    sys.exit("Error: Could not import 'CoriolixSensorConfig' from 'generate_grafana_live_stream.py'.")

# Regex to grab the first word of a DAS record (the data_id)
# Matches: "data_id ..." at start of line
DATA_ID_REGEX = re.compile(r'^\s*(\w+)')


class SensorIDMapper:
    def __init__(self, api_url=None, timeout=3.0):
        self.sensor_config = CoriolixSensorConfig(api_url=api_url)
        self.timeout = timeout
        self.lock = threading.Lock()
        self.mapping = {}

    def _probe_port(self, sensor_id, port):
        """
        Listens on a UDP port for a single packet to extract the data_id.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(self.timeout)

        try:
            # Bind to all interfaces on the specific port
            sock.bind(('', int(port)))

            # Listen for one packet
            data, _ = sock.recvfrom(4096)
            decoded = data.decode('utf-8', errors='ignore').strip()

            # Extract data_id using regex
            match = DATA_ID_REGEX.match(decoded)
            if match:
                found_id = match.group(1)
                with self.lock:
                    self.mapping[sensor_id] = found_id
            else:
                # If we received data but couldn't parse ID, log strict warning?
                # Or just assume it matches API ID? For now, we skip.
                pass

        except socket.timeout:
            # No data received within timeout
            pass
        except Exception as e:
            sys.stderr.write(f"Error probing {sensor_id} on port {port}: {e}\n")
        finally:
            sock.close()

    def build_mapping(self):
        """
        Main driver: fetches active sensors, probes them in parallel,
        and returns the ID mapping.
        """
        # 1. Get active sensors from API (reusing existing logic)
        # Note: We want the raw hardware ID to map FROM.
        # get_active_sensor_ids() returns slugs if it finds them, but we want the objects
        # to ensure we have the port. We'll use the internal _fetch_all_sensors cache logic
        # exposed via get_sensor_metadata or just replicate the loop efficiently.

        # Let's verify what get_active_sensor_ids returns. It returns a list of identifiers.
        # We need the PORT.

        # Efficient approach: Fetch raw list once using the helper
        all_sensors = self.sensor_config._fetch_all_sensors()

        threads = []

        sys.stderr.write(f"Scanning {len(all_sensors)} sensors for active UDP streams...\n")

        for s in all_sensors:
            if not isinstance(s, dict): continue

            # Check Enabled
            val = s.get('enabled')
            is_enabled = str(val).lower() == 'true' if isinstance(val, (str, bool)) else False
            if not is_enabled: continue

            # Check Port
            port = s.get('transmit_port')
            if not port: continue

            # Identify the API ID (This is our Key)
            # We use sensor_id (hardware ID) as the reliable key from the API side
            api_id = s.get('sensor_id')
            if not api_id: continue

            # Spawn a thread to probe this port
            t = threading.Thread(target=self._probe_port, args=(api_id, port))
            t.start()
            threads.append(t)

        # Wait for all probes to finish
        for t in threads:
            t.join()

        sys.stderr.write(f"Probe complete. Found signals on {len(self.mapping)} ports.\n")

        # Filter: Only return mappings where the ID *differs* or return all?
        # Returning all is safer for the consumer script to just look up.
        # But for readability, we might sort them.
        return dict(sorted(self.mapping.items()))


if __name__ == "__main__":
    mapper = SensorIDMapper()
    id_map = mapper.build_mapping()

    print(yaml.dump(id_map, sort_keys=False, default_flow_style=False))
