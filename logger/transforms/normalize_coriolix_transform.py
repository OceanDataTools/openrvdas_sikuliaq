#!/usr/bin/env python3

import logging
import sys
import math
import json
from os.path import dirname, realpath

sys.path.append(dirname(dirname(dirname(realpath(__file__)))))
from logger.utils.das_record import DASRecord  # noqa: E402
from logger.transforms.transform import Transform  # noqa: E402


class NormalizeCoriolixTransform(Transform):
    """
    Normalizes a DASRecord for standard consumption:
    1. Converts numeric strings to proper int/float types (skipping specified fields).
    2. Optionally creates normalized Decimal Degree fields from NMEA-style coordinates.
    """

    def __init__(self, lat_lon_map=None, skip_fields=None):
        """
        lat_lon_map: Dict mapping { new_field_name: (raw_coord_field, raw_hemisphere_field) }
                     Example: {'latitude': ('lat_nmea', 'lat_dir')}
        skip_fields: List of field names to skip during number normalization.
                     These fields remain in the record as strings.
        """
        self.lat_lon_map = lat_lon_map or {}
        self.skip_fields = set(skip_fields or [])

    def _try_convert_number(self, value):
        if isinstance(value, str):
            # Check for simple integer (handles negative signs)
            if value.lstrip('-').isdigit():
                return int(value)
            # Check for float
            try:
                return float(value)
            except ValueError:
                pass
        return value

    def _nmea_to_decimal(self, value, direction):
        """
        Convert NMEA dddmm.mmmm format to Decimal Degrees.
        e.g. 2156.8986, S -> -21.94831
        """
        try:
            val_float = float(value)
            # NMEA format is D...DMM.mmmm
            # Divide by 100 to separate Degrees (integer part) from Minutes (decimal part)
            degrees = math.floor(val_float / 100)
            minutes = val_float % 100
            decimal_degrees = degrees + (minutes / 60.0)

            if str(direction).upper() in ['S', 'W']:
                decimal_degrees *= -1.0

            return decimal_degrees
        except (ValueError, TypeError):
            # logging.debug(f'Could not convert NMEA coord: {value} {direction}')
            return None

    def transform(self, record):
        """
        Accepts DASRecord, dict, or json string. Returns clean DASRecord.
        """
        if not self.can_process_record(record):
            return self.digest_record(record)

        # 1. Normalize input to DASRecord
        if isinstance(record, str):
            try:
                record = DASRecord(json_str=record)
            except json.JSONDecodeError:
                return None
        elif isinstance(record, dict):
            # Handle flat dict or dict with 'fields'
            if 'fields' in record:
                record = DASRecord(fields=record['fields'],
                                   timestamp=record.get('timestamp'),
                                   data_id=record.get('data_id'),
                                   message_type=record.get('message_type'))
            else:
                # Flat dict copy
                rec_copy = record.copy()
                ts = rec_copy.pop('timestamp', None)
                did = rec_copy.pop('data_id', None)
                mtype = rec_copy.pop('message_type', None)
                record = DASRecord(fields=rec_copy, timestamp=ts, data_id=did, message_type=mtype)

        if not record or not record.fields:
            return record

        # Work on a copy of fields to avoid mutating original if passed by ref
        new_fields = record.fields.copy()

        # 2. Number Conversion (Respecting skip_fields)
        for k, v in new_fields.items():
            if k in self.skip_fields:
                continue  # Leave as is (e.g. string)
            new_fields[k] = self._try_convert_number(v)

        # 3. Lat/Lon Normalization
        # lat_lon_map format: { 'final_name': ('raw_val_name', 'raw_dir_name') }
        for final_name, (raw_val_key, raw_dir_key) in self.lat_lon_map.items():
            if raw_val_key in new_fields:
                raw_val = new_fields.get(raw_val_key)
                raw_dir = new_fields.get(raw_dir_key)

                decimal_val = self._nmea_to_decimal(raw_val, raw_dir)

                if decimal_val is not None:
                    new_fields[final_name] = decimal_val

        record.fields = new_fields
        return record
