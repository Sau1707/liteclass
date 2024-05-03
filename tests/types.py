import unittest
import ezstorage as ez
from datetime import datetime, date, timedelta

DB = ez.Sqlite(":memory:")


@DB.useTable("test_table")
class Test(ez.Table):
    integer: int            # Saved as an INTEGER
    float: float            # Saved as a REAL
    string: str             # Saved as a TEXT
    boolean: bool           # Saved as an INTEGER
    datetime: datetime      # Saved as a TEXT
    date: date              # Saved as a TEXT    
    timedelta: timedelta    # Saved as a TEXT
    list: list              # Saved as a TEXT
    dict: dict              # Saved as a TEXT
    set: set                # Saved as a TEXT
    tuple: tuple            # Saved as a TEXT



DB.create_table(Test)

test_with_values = Test(
    integer=0,
    float=0.0,
    string="",
    boolean=False,
    datetime=datetime(1, 1, 1),
    date=date(1, 1, 1),
    timedelta=timedelta(0),
    list=[],
    dict={},
    set=set(),
    tuple=()
)
test_with_values.save()

test_without_values = Test(
    integer=None,
    float=None,
    string=None,
    boolean=None,
    datetime=None,
    date=None,
    timedelta=None,
    list=None,
    dict=None,
    set=None,
    tuple=None
)
test_without_values.save()

class TestTypes(unittest.TestCase):
    def test_type_int(self):
        """Test the type int"""
        content = Test.where(lambda: Test.integer == 0)[0]
        self.assertEqual(content.key, 0)

        none_content = Test.where(lambda: Test.integer == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_float(self):
        """Test the type float"""
        content = Test.where(lambda: Test.float == 0.0)[0]
        self.assertEqual(content.key, 0.0)

        none_content = Test.where(lambda: Test.float == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_str(self):
        """Test the type str"""
        content = Test.where(lambda: Test.string == "")[0]
        self.assertEqual(content.key, "")

        none_content = Test.where(lambda: Test.string == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_bool(self):
        """Test the type bool"""
        content = Test.where(lambda: Test.boolean == False)[0]
        self.assertEqual(content.key, False)

        none_content = Test.where(lambda: Test.boolean == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_datetime(self):
        """Test the type datetime"""
        content = Test.where(lambda: Test.datetime == datetime(1, 1, 1))[0]
        self.assertEqual(content.key, datetime(1, 1, 1))

        none_content = Test.where(lambda: Test.datetime == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_date(self):
        """Test the type date"""
        content = Test.where(lambda: Test.date == date(1, 1, 1))[0]
        self.assertEqual(content.key, date(1, 1, 1))

        none_content = Test.where(lambda: Test.date == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_timedelta(self):
        """Test the type timedelta"""
        content = Test.where(lambda: Test.timedelta == timedelta(0))[0]
        self.assertEqual(content.key, timedelta(0))

        none_content = Test.where(lambda: Test.timedelta == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_list(self):
        """Test the type list"""
        content = Test.where(lambda: Test.list == [])[0]
        self.assertEqual(content.key, [])

        none_content = Test.where(lambda: Test.list == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_dict(self):
        """Test the type dict"""
        content = Test.where(lambda: Test.dict == {})[0]
        self.assertEqual(content.key, {})

        none_content = Test.where(lambda: Test.dict == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_set(self):
        """Test the type set"""
        content = Test.where(lambda: Test.set == set())[0]
        self.assertEqual(content.key, set())

        none_content = Test.where(lambda: Test.set == None)[0]
        self.assertEqual(none_content.key, None)

    def test_type_tuple(self):
        """Test the type tuple"""
        content = Test.where(lambda: Test.tuple == ())[0]
        self.assertEqual(content.key, ())

        none_content = Test.where(lambda: Test.tuple == None)[0]
        self.assertEqual(none_content.key, None)


   
if __name__ == "__main__":
    unittest.main()