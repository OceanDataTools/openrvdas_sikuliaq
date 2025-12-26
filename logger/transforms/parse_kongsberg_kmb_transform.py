#!/usr/bin/env python3

import logging
import sys
import struct
import datetime
from os.path import dirname, realpath

sys.path.append(dirname(dirname(dirname(realpath(__file__)))))
from logger.utils.das_record import DASRecord  # noqa: E402
from logger.transforms.transform import Transform  # noqa: E402


class ParseKongsbergKMBTransform(Transform):
    """
    Parses Kongsberg Seapath '#KMB' binary datagrams into DASRecords.

    Format:
      Start ID         (4s)  : #KMB
      Datagram length  (H)   : uint16
      Datagram version (H)   : uint16
      UTC seconds      (I)   : uint32
      UTC nanoseconds  (I)   : uint32
      Status           (I)   : uint32
      Latitude         (d)   : double (8 bytes)
      Longitude        (d)   : double (8 bytes)
      Ellipsoid height (f)   : float (4 bytes)
      Roll             (f)   : float (4 bytes)
      Pitch            (f)   : float (4 bytes)
      Heading          (f)   : float (4 bytes)
      Heave            (f)   : float (4 bytes)
      Roll rate        (f)   : float (4 bytes)

    Total Expected Size: 60 bytes
    """

    def __init__(self, data_id='seapath_kmb'):
        self.data_id = data_id
        # Struct Format:
        # > : Big Endian
        # 4s: Start ID (#KMB)
        # H : Length (uint16)
        # H : Version (uint16)
        # I : Seconds (uint32)
        # I : Nanoseconds (uint32)
        # I : Status (uint32)
        # d : Latitude (double)
        # d : Longitude (double)
        # f : Height (float)
        # f : Roll (float)
        # f : Pitch (float)
        # f : Heading (float)
        # f : Heave (float)
        # f : Roll Rate (float)
        self.struct_fmt = '>4sHHIIIddffffff'
        self.struct_size = struct.calcsize(self.struct_fmt)

    def _calc_timestamp(self, seconds, nanoseconds):
        """
        Convert Seapath UTC seconds/nanoseconds to Unix Timestamp.
        Note: Seapath epoch is usually Unix Epoch (Jan 1 1970).
        """
        return seconds + (nanoseconds / 1e9)

    def transform(self, record):
        if not self.can_process_record(record):
            return self.digest_record(record)

        if not isinstance(record, bytes):
            # If we received a hex string by mistake, try to convert it
            if isinstance(record, str):
                try:
                    record = bytes.fromhex(record)
                except ValueError:
                    return None
            else:
                return None

        # 1. Sanity Check Size
        if len(record) < self.struct_size:
            # Not enough bytes for a full packet
            return None

        try:
            # 2. Unpack
            # We only unpack the first 60 bytes (self.struct_size)
            values = struct.unpack(self.struct_fmt, record[:self.struct_size])

            (start_id, dgm_len, version, utc_sec, utc_nano, status,
             lat, lon, height, roll, pitch, heading, heave, roll_rate) = values

            # 3. Validation
            # Verify Start ID is #KMB
            if start_id != b'#KMB':
                # If the packet isn't aligned, we might want to search for #KMB
                # But for now, just reject.
                return None

            # 4. Create Fields
            fields = {
                'status': status,
                'latitude': lat,
                'longitude': lon,
                'ellipsoid_height_m': height,
                'roll_deg': roll,
                'pitch_deg': pitch,
                'heading_deg': heading,
                'heave_m': heave,
                'roll_rate_deg_s': roll_rate
            }

            # 5. Timestamp
            ts = self._calc_timestamp(utc_sec, utc_nano)

            return DASRecord(
                data_id=self.data_id,
                message_type='kmb',
                timestamp=ts,
                fields=fields
            )

        except struct.error:
            logging.warning('Failed to unpack #KMB datagram')
            return None
