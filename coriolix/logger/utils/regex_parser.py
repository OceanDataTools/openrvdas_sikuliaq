#!/usr/bin/env python3

"""Tools for parsing NMEA and other text records using regex.
"""
import datetime
import json
import logging
import re
import pprint
import sys

from os.path import dirname, realpath

from logger.utils.das_record import DASRecord

# Append openrvdas root to syspath prior to importing openrvdas modules
sys.path.append(dirname(dirname(dirname(realpath(__file__)))))

DEFAULT_RECORD_FORMAT = r"^(?P<timestamp>[0-9TZ:\-\.]*)\s+(?P<data_id>\w+)\s*(?P<field_string>(.|\r|\n)*)"


################################################################################
class RegexParser:
    ############################
    def __init__(self,
                 record_format=None,
                 field_patterns=None,
                 return_das_record=False,
                 return_json=False,
                 quiet=False):
        """Create a parser that will parse field values out of a text record
        and return either a Python dict of data_id, timestamp and fields,
        a JSON encoding of that dict, or a binary DASRecord.
        ```
        record_format - string for re.match() to use to break out data_id
            and timestamp from the rest of the message. By default this will
            look for 'data_id timestamp field_string', where 'field_string'
            is a str containing the fields to be parsed.

        field_patterns
            If not None, either
            - a list of regex patterns to be tried
            - a dict of message_type:regex patterns to be tried. When one
              matches, the record's message_type is set accordingly.

        return_json - return the parsed fields as a JSON encoded dict

        return_das_record - return the parsed fields as a DASRecord object

        quiet - if not False, don't complain when unable to parse a record.
        ```
        """
        self.quiet = quiet
        self.field_patterns = field_patterns
        self.record_format = record_format or DEFAULT_RECORD_FORMAT
        self.compiled_record_format = re.compile(self.record_format)
        self.return_das_record = return_das_record
        self.return_json = return_json
        if return_das_record and return_json:
            raise ValueError('Only one of return_json and return_das_record '
                             'may be true.')

        # If we've been explicitly given the field_patterns we're to use for
        # parsing, compile them now.
        if field_patterns:
            if isinstance(field_patterns, list):
                self.compiled_field_patterns = [
                    re.compile(pattern)
                    for pattern in field_patterns
                ]
            elif isinstance(field_patterns, dict):
                self.compiled_field_patterns = {
                    message_type: re.compile(pattern)
                    for (message_type, pattern) in field_patterns.items()
                }
            else:
                raise ValueError('field_patterns must either be a list of patterns or '
                                 'dict of message_type:pattern pairs. Found type '
                                 f'{type(field_patterns)}')

    ############################
    def parse_record(self, record):
        """Parse an id-prefixed text record into a Python dict of
        data_id, timestamp and fields.
        """
        if not record:
            return None
        if not type(record) is str:
            logging.info('Record is not a string: "%s"', record)
            return None
        try:
            # logging.error(record)
            parsed_record = self.compiled_record_format.match(record).groupdict()
            # print(parsed_record)
            # logging.error(parsed_record)
        except (ValueError, AttributeError):
            if not self.quiet:
                logging.warning('Unable to parse record into "%s"', self.record_format)
                logging.warning('Record: %s', record)
            return None

        if parsed_record is None:
            return None

        # Extract the data_id
        data_id = parsed_record.get('data_id', None)

        # Convert timestamp to numeric, if it's there
        timestamp_text = parsed_record.get('timestamp', None)
        if timestamp_text is not None:
            timestamp = self.convert_timestamp(timestamp_text)
            if timestamp is not None:
                parsed_record['timestamp'] = timestamp

        # Extract the field string we're going to parse;
        # remove trailing whitespace.
        field_string = parsed_record.get('field_string', None).rstrip()
        if field_string is not None:
            del parsed_record['field_string']

        message_type = None
        fields = {}
        if field_string:
            # If we've been given a set of field_patterns to apply,
            # use the first that matches.

            # Shortcut that lets us iterate through a list or a dict with the same
            # invocation. With a list, it returns (None, value); with a dict it
            # returns (key, value).
            iterate_patterns = lambda obj: (obj.items() if isinstance(obj, dict) else
                                           ((None, v) for v in obj))
            if self.field_patterns:
                for message_type, pattern in iterate_patterns(self.compiled_field_patterns):
                    start_time = datetime.datetime.now()
                    try:
                        try_parse = pattern.match(field_string)
                        # Did we find a parse that matched?
                        # If so, return its fields
                        if try_parse:
                            fields = try_parse.groupdict()
                            diff_time = (datetime.datetime.now() - start_time)
                            duration = diff_time.total_seconds() * 1000
                            break
                    except Exception as e:
                        logging.error(e)
                    diff_time = (datetime.datetime.now() - start_time)
                    duration = diff_time.total_seconds() * 1000

        if fields:
            parsed_record['fields'] = fields

        logging.debug('Created parsed record: %s', pprint.pformat(parsed_record))

        metadata = None

        # What are we going to do with the result we've created?
        if self.return_das_record:
            try:
                return DASRecord(data_id=data_id, timestamp=timestamp,
                                 message_type=message_type,
                                 fields=fields, metadata=metadata)
            except KeyError:
                return None

        elif self.return_json:
            return json.dumps(parsed_record)
        else:
            return parsed_record

    ############################
    def convert_timestamp(self, datetime_text):
        """Validates a datetime string and converts to numeric.
        """

        DEFAULT_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

        try:
            datetime_ti = datetime.datetime.strptime(
                datetime_text, DEFAULT_FORMAT)
        except ValueError:
            logging.debug("Incorrect datetime format.")
            return None

        if datetime_ti:
            timestamp = datetime_ti.timestamp()
            return timestamp
