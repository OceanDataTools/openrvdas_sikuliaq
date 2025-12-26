#!/usr/bin/env python3

import struct
import time
import math
import sys
import argparse
import socket

def generate_data(count=100, rate_hz=20, udp_port=None):
    # Parameters
    interval = 1.0 / rate_hz
    
    # Struct Format (Big Endian per Kongsberg spec)
    # 4s: #KMB
    # H: Length (60)
    # H: Version (1)
    # I: UTC Seconds
    # I: UTC Nano
    # I: Status
    # d: Lat
    # d: Lon
    # f: Height, Roll, Pitch, Heading, Heave, RollRate
    struct_fmt = '>4sHHIIIddffffff'
    packet_len = 60
    
    # Setup UDP socket if needed
    sock = None
    if udp_port:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print(f"Sending {count} packets to localhost:{udp_port} at {rate_hz}Hz...", file=sys.stderr)
    
    try:
        start_real_time = time.time()
        
        for i in range(count):
            # 1. Calculate Timestamp
            # We use current time so the data is "fresh" for Grafana
            current_time = time.time()
            utc_sec = int(current_time)
            utc_nano = int((current_time - utc_sec) * 1e9)

            # 2. Simulate Motion (Sine waves for realism)
            # Lat/Lon drift slowly NE
            lat = 45.0 + (i * 0.00001)
            lon = -125.0 + (i * 0.00001)
            
            # Heave: 1 meter wave, 5 second period
            heave = 1.0 * math.sin(i * 0.1)
            
            # Roll: +/- 5 degrees, 8 second period
            roll = 5.0 * math.sin(i * 0.05)
            
            # Pitch: +/- 2 degrees
            pitch = 2.0 * math.cos(i * 0.05)
            
            # Heading: Constant turn to starboard
            heading = (180.0 + (i * 0.1)) % 360.0
            
            ellipsoid_height = 10.0 + heave
            roll_rate = 0.5 * math.cos(i * 0.05)
            status = 0 # OK

            # 3. Pack Binary Data
            packet = struct.pack(struct_fmt,
                                 b'#KMB',      # Start ID
                                 packet_len,   # Length
                                 1,            # Version
                                 utc_sec,      # Time
                                 utc_nano,     # Time
                                 status,       # Status
                                 lat, lon,     # Position
                                 ellipsoid_height, 
                                 roll, pitch, heading, 
                                 heave, roll_rate)

            # 4. Output
            if sock:
                # Send Raw Binary via UDP
                sock.sendto(packet, ('127.0.0.1', udp_port))
            else:
                # Print Hex String to Stdout
                print(packet.hex())
                sys.stdout.flush()

            # 5. Maintain Rate (Sleep)
            # Calculate how much time this iteration took and sleep the remainder
            elapsed = time.time() - start_real_time
            target_time = (i + 1) * interval
            sleep_time = target_time - elapsed
            
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\nStopping...", file=sys.stderr)
    finally:
        if sock:
            sock.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate synthetic Kongsberg #KMB datagrams.")
    parser.add_argument('--udp', type=int, help="Send data via UDP to localhost:<PORT> instead of stdout.")
    parser.add_argument('--count', type=int, default=1000, help="Number of packets to generate (default: 1000).")
    parser.add_argument('--rate', type=int, default=20, help="Data rate in Hz (default: 20).")
    
    args = parser.parse_args()
    
    generate_data(count=args.count, rate_hz=args.rate, udp_port=args.udp)
