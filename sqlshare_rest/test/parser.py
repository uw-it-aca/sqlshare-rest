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
        self.assertEquals(['a','b','c','d'], p.headers())

        p.guess("9,8,7,6\n0,1,2,3\n4,5,6,7")
        self.assertFalse(p.has_header_row())
        self.assertEquals(['Column1','Column2','Column3','Column4'], p.headers())

        p.guess("a,b,5,6\n0,1,2,3\n4,5,6,7")
        self.assertFalse(p.has_header_row())

        p.guess("a,b,b,d\n0,1,2,3\n4,5,6,7")
        self.assertTrue(p.has_header_row())
        self.assertEquals(['a','b','b1','d'], p.headers())

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
