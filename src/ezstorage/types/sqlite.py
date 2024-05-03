from .__layout__ import LayoutTypes
from datetime import datetime, date, timedelta


class SQLiteTypes(LayoutTypes):
    """SQLite types"""

    TYPES = {
        # Numbers
        int: 'INTEGER',
        float: 'REAL',
        str: 'TEXT',
        bool: 'INTEGER',
        # Date and time
        datetime: 'TEXT',
        date: 'TEXT',
        timedelta: 'TEXT',
        # Collections
        list: 'TEXT',
        dict: 'TEXT',
        set: 'TEXT',
        tuple: 'TEXT'
    }

    def encode(self, value, _type):
        """Encode the type, return the database compatible type"""
        if value is None:
            return None
        
        if _type in (datetime, date, timedelta):
            return value.isoformat()
        
        if _type in (list, dict, set, tuple):
            return str(value)
        
        if _type is bool:
            return int(value)
        
        return value

    def decode(self, value, _type):
        """Decode the type, return the python compatible type"""
        if value is None:
            return None

        return value