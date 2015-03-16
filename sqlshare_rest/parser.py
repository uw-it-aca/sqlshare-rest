import csv
from StringIO import StringIO


class Parser(object):
    def __init__(self):
        self._has_header = None
        self._delimiter = None
        self._headers = None
        self._handle = None

    def set_defaults(self):
        self._has_header = False
        self._headers = []
        self._delimiter = ','

    def guess(self, content):
        self.set_defaults()
        self._has_header = csv.Sniffer().has_header(content)
        self._delimiter = csv.Sniffer().sniff(content).delimiter

        data = StringIO(content)
        if self._has_header:
            self._headers = self._get_headers_from_handle(data)
        else:
            count = len(csv.reader(data).next())
            self._headers = self.generate_column_names(count)

    def parse(self, handle):
        if self._has_header is None:
            raise Exception("Need to set has_header, or call guess()")
        if self._delimiter is None:
            raise Exception("Need to set delimiter, or call guess()")

        handle.seek(0)
        self._handle = handle
        if self.has_header_row():
            # XXX - make this overridable?
            self._headers = self._get_headers_from_handle(handle)

    def _get_headers_from_handle(self, handle):
        return self.make_unique_columns(csv.reader(handle).next())

    def make_unique_columns(self, names):
        seen_names = {}
        unique = []
        for name in names:
            if name in seen_names:
                new_name = "%s%i" % (name, seen_names[name])
                unique.append(new_name)
                seen_names[name] = seen_names[name] + 1
                seen_names[new_name] = 1
            else:
                unique.append(name)
                seen_names[name] = 1

        return unique

    def generate_column_names(self, count):
        names = []
        for i in range(1, count+1):
            names.append("Column%s" % i)
        return names

    def has_header_row(self, value=None):
        if value is not None:
            self._has_header = value
        return self._has_header

    def headers(self):
        return self._headers

    def delimiter(self, value=None):
        if value is not None:
            self._delimiter = value
        return self._delimiter

    # To make this iterable - intended to make it so we can be somewhat
    # low-memory, even on large files
    def __iter__(self):
        return self

    def next(self):
        return csv.reader(self._handle, delimiter=self.delimiter()).next()
