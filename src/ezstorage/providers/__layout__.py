from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import dataclass_transform, TYPE_CHECKING
if TYPE_CHECKING:
    from ..table import Table


class LayoutProvider(ABC):
    """
        Abstract class for the database provider
        Each provider extend this class and implement the methods that are provider specific
    """
    __tables__ = []
    __classes__ = []

    @dataclass_transform()
    def useTable(self, table_name: str):
        def decorator(cls: "Table"):
            # Apply the dataclass decorator
            cls = dataclass(cls)

            # Check that all the keys have a type and that the value is of the right type
            for key in cls.__dict__:
                if key.startswith("__"):
                    continue
                assert key in cls.__annotations__, f"Key '{key}' does not have a type in the class {cls.__name__}"
                assert isinstance(cls.__dict__[key], cls.__annotations__[key]), f"Value {cls.__dict__[key]} is not of type {cls.__annotations__[key]}"

            # Assign the table name to the class
            cls.__table__ = table_name

            # Assign the current provider to the class
            cls.__provider__ = self

            # Check if the table exists
            if table_name in self.__tables__:
                cls.__exist__ = True
                cls.__schema__ = self._get_schema(cls)

            # Check that columns have the right type
            for key in cls.__annotations__:
                if key in cls.__schema__:
                    msg = f"Column {key} has type {cls.__annotations__[key]} in the class but has type {cls.__schema__[key]} in the database"
                    assert self._convert_type(cls.__annotations__[key]) == cls.__schema__[key],msg

            self.__classes__.append(cls)
            return cls
        return decorator

    @abstractmethod
    def _get_schema(self, table) -> dict:
        """Return the schema of the table"""

    @abstractmethod
    def create_table(self, table):
        """Create a table in the database"""

    def create_tables(self):
        """Create the table in the database"""
        for table in self.__classes__:
            self.create_table(table)

    @abstractmethod
    def drop_table(self, table):
        """Drop the table from the database"""

    def drop_tables(self):
        """Drop the table from the database"""
        for table in self.__classes__:
            self.drop_table(table)

    @abstractmethod
    def update_table(self, table):
        """Update the table schema in the database"""

    def update_tables(self):
        """Update the table schema in the database"""
        for table in self.__classes__:
            self.update_table(table)

    @abstractmethod
    def commit(self):
        """Commit the changes to the database"""

    @abstractmethod
    def close(self):
        """Close the connection to the database"""

    ##########################################
    # Database operations
    ##########################################
    @abstractmethod
    def _insert(self, obj, table):
        """Insert a row into the table"""

    @abstractmethod
    def execute(self, query):
        """Select the table from the database"""

    ##########################################
    # Utility functions
    ##########################################
    @abstractmethod
    def _convert_type(self, type) -> str:
        """Convert the type to the database type"""

    @abstractmethod
    def _get_missing_columns(self, table) -> list:
        """Return the missing columns"""

    @abstractmethod
    def _get_extra_columns(self, table) -> list:
        """Return the extra columns"""

    @abstractmethod
    def _get_changed_columns(self, table) -> list:
        """Return the changed columns"""

    @abstractmethod
    def _tokens_to_sql(self, tokens, table) -> str:
        """Convert the token to a sql string"""

    ##########################################
    # Magic methods
    ##########################################
    @abstractmethod
    def __del__(self):
        """Close the connection to the database"""

    @abstractmethod
    def __enter__(self):
        """Enter the context"""

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the context"""