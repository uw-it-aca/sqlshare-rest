from django.test import TestCase
from sqlshare_rest.parser import Parser
import six
if six.PY2:
    from StringIO import StringIO
elif six.PY3:
    from io import StringIO

class TestParser(TestCase):
    def test_headers(self):
        # This is based on existing behavior in the C# app, not necessarily
        # desired behavior
        p = Parser()
        p.guess("a,b,c,d\n0,1,2,3\n4,5,6,7")
        self.assertTrue(p.has_header_row())
        self.assertEquals(['a','b','c','d'], p.column_names())

        p.guess("9,8,7,6\n0,1,2,3\n4,5,6,7")
        self.assertFalse(p.has_header_row())
        self.assertEquals(['Column1','Column2','Column3','Column4'], p.column_names())

        p.guess("a,b,5,6\n0,1,2,3\n4,5,6,7")
        self.assertFalse(p.has_header_row())

        p.guess("a,b,b,d\n0,1,2,3\n4,5,6,7")
        self.assertTrue(p.has_header_row())
        self.assertEquals(['a','b','b1','d'], p.column_names())

    def test_overrides(self):
        p = Parser()
        p.delimiter(",")
        p.has_header_row(True)

        handle = StringIO("a,b,b,d\n0,1,2,3\n4,5,6,7")
        p.parse(handle)

        data = [['0', '1', '2', '3', ], ['4', '5', '6', '7',]]
        index = 0
        for row in p:
            self.assertEquals(row, data[index])
            index = index + 1

        # Override delimiter
        p.delimiter("|")
        p.parse(handle)

        data = [['0,1,2,3',], ['4,5,6,7',]]
        index = 0
        for row in p:
            self.assertEquals(row, data[index])
            index = index + 1

        # Override header row
        p.has_header_row(False)
        p.parse(handle)

        data = [['a,b,b,d',], ['0,1,2,3',], ['4,5,6,7',]]
        index = 0
        for row in p:
            self.assertEquals(row, data[index])
            index = index + 1

    def test_non_square(self):
        p = Parser()
        p.delimiter(",")
        p.has_header_row(True)

        handle = StringIO("a,b,b,d\n0,1\n4")
        p.parse(handle)

        data = [['0', '1', ], ['4',]]
        index = 0
        for row in p:
            self.assertEquals(row, data[index])
            index = index + 1

    def test_unique_column_names(self):
        self.assertEquals(Parser().make_unique_columns(["a", "a", "a", "a"]), ["a", "a1", "a2", "a3"])
        self.assertEquals(Parser().make_unique_columns(["a", "a", "a1", "a1"]), ["a", "a1", "a11", "a12"])


    def test_column_types(self):
        p = Parser()
        p.delimiter(",")
        p.has_header_row(False)

        # Basic ints
        handle = StringIO("0,1,2\n3,4,5")
        p.parse(handle)
        p.clear_column_types()
        self.assertEquals(p.column_types(), [{ "type": "int" }, { "type": "int" }, { "type": "int" }])

        # Basic floats
        handle = StringIO("0.1,1.1,2.1\n3.1,4.1,5.1")
        p.parse(handle)
        p.clear_column_types()
        self.assertEquals(p.column_types(), [{ "type": "float" }, { "type": "float" }, { "type": "float" }])

        # Basic text
        handle = StringIO("aa,ba,ca\nab,bb,cc")
        p.parse(handle)
        p.clear_column_types()
        self.assertEquals(p.column_types(), [{ "type": "text", "max": 2}, { "type": "text", "max": 2},{ "type": "text", "max": 2},])

        # Int falling back to float
        handle = StringIO("0,1,2\n3,4.1,5")
        p.parse(handle)
        p.clear_column_types()
        self.assertEquals(p.column_types(), [{ "type": "int" }, { "type": "float" }, { "type": "int" }])

        # Int falling back to text
        handle = StringIO("0,1,2\n3,aa,5")
        p.parse(handle)
        p.clear_column_types()
        self.assertEquals(p.column_types(), [{ "type": "int" },  { "type": "text", "max": 2}, { "type": "int" }])

        # floats falling back to text
        handle = StringIO("0.1,1.1,2.1\naaa,4.1,5.1")
        p.parse(handle)
        p.clear_column_types()
        self.assertEquals(p.column_types(), [{ "type": "text", "max": 3 }, { "type": "float" }, { "type": "float" }])

        # Int falling back to float faling back to text
        handle = StringIO("0,1,2\n3,4.1,5\n1,1aa2,3")
        p.parse(handle)
        p.clear_column_types()
        self.assertEquals(p.column_types(), [{ "type": "int" }, { "type": "text", "max": 4 }, { "type": "int" }])

        # keeps growing....
        handle = StringIO("a\nab\nabc\nabcd\nabcde")
        p.parse(handle)
        p.clear_column_types()
        self.assertEquals(p.column_types(), [{ "type": "text", "max": 5}])

    def test_data_handle(self):
        p = Parser()
        p.delimiter(",")
        p.has_header_row(False)
        handle = StringIO("0,1.1,a\n3,4.1,bbbsd")
        p.parse(handle)
        data_handle = p.get_data_handle()
        self.assertEquals(data_handle.next(), [0, 1.1, "a"])
        self.assertEquals(data_handle.next(), [3, 4.1, "bbbsd"])

