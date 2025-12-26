#!/usr/bin/env python3
"""
Modified version of CORIOLIX RegexTransform that accepts a {message_type:field_pattern} dict
as well as a simple list of field_patterns.
"""
import sys
from os.path import dirname, realpath
from datetime import datetime

from ..utils import regex_parser

from logger.transforms.transform import Transform

sys.path.append(dirname(dirname(dirname(realpath(__file__)))))


###############################################################################
class RegexTransform(Transform):
    """Parse a "<timestamp> <data_id> <message>" record and return
    corresponding dict of values (or JSON or DASRecord if specified)."""
    def __init__(self, record_format=None, field_patterns=None,
                 return_json=False, return_das_record=False,
                 quiet=False):
        """
        ```
        record_format
                If not None, a custom record format to use for parsing records.
                The default, defined in logger/utils/regex_parser.py, is:
                ^(?P<timestamp>[0-9TZ:\-\.]*)\s+(?P<data_id>\w+)\s*(?P<field_string>(.|\r|\n)*)

        field_patterns
            - a list of regex patterns to be tried
            - a dict of message_type:regex patterns to be tried. When one
              matches, the record's message_type is set accordingly.

        return_json
                Return a JSON-encoded representation of the dict
                instead of a dict itself.

        return_das_record
                Return a DASRecord object.

        quiet - if not False, don't complain when unable to parse a record.
        ```
        """
        self.parser = regex_parser.RegexParser(
            record_format=record_format,
            field_patterns=field_patterns,
            return_json=return_json,
            return_das_record=return_das_record,
            quiet=quiet)

    ############################
    def transform(self, record):
        """Parse record and return DASRecord."""

        if record is None:
            return None

        # If we've got a list, hope it's a list of records. Recurse,
        # calling transform() on each of the list elements in order and
        # return the resulting list.
        if type(record) is list:
            results = []
            for single_record in record:
                results.append(self.transform(single_record))
            return results

        parsed = self.parser.parse_record(record)

        return parsed
