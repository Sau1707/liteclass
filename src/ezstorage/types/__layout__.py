from abc import ABC, abstractmethod
from datetime import datetime, date, timedelta


class LayoutTypes(ABC):
    """Abstract class for layout types"""

    # Template for the types, the key is the python type and the value is the database compatible type
    TYPES = {
        # Numbers
        int: None,
        float: None,
        str: None,
        bool: None,
        # Date and time
        datetime: None,
        date: None,
        timedelta: None,
        # Collections
        list: None,
        dict: None,
        set: None,
        tuple: None
    }

    def __init__(self):
        # Fill the template with the database compatible types
        assert all([self.TYPES[key] is not None for key in self.TYPES]), "Some types are not defined"

    @abstractmethod
    def encode(self, value, _type):
        """Encode the type, return the database compatible type"""
    
    @abstractmethod
    def decode(self, value, _type):
        """Decode the type, return the python compatible type"""