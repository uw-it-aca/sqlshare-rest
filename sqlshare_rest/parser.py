import csv
import six
import io
import chardet

if six.PY2:
    from StringIO import StringIO
elif six.PY3:
    from io import StringIO


class Parser(object):
    def __init__(self):
        self._has_header = None
        self._delimiter = None
        self._column_names = None
        self._handle = None
        self._column_types = None

    def set_defaults(self):
        self._has_header = False
        self._column_names = []
        self._delimiter = ','

    def guess(self, content):
        self.set_defaults()
        try:
            self._has_header = csv.Sniffer().has_header(content)
            self._delimiter = csv.Sniffer().sniff(content,
                                                  delimiters=",\t").delimiter
        except Exception as ex:
            # In some non-square datasets, csv fails to detect "," as the
            # delimiter, even if it seems like a pretty clear choice
            self._delimiter = ","
            # This can also mess up header detection :(
            self._has_header = False

        data = StringIO(content)
        if self._has_header:
            self._column_names = self._get_headers_from_handle(data)
        else:
            count = len(self._next(self._get_csv_reader(data)))
            self._column_names = self.generate_column_names(count)

    def parse(self, handle):
        if self._has_header is None:
            raise Exception("Need to set has_header, or call guess()")
        if self._delimiter is None:
            raise Exception("Need to set delimiter, or call guess()")

        handle.seek(0)
        self._handle = handle
        if self.has_header_row():
            # XXX - make this overridable?
            self._column_names = self._get_headers_from_handle(handle)
        else:
            count = len(self._next(self._get_csv_reader(handle)))
            handle.seek(0)
            self._column_names = self.generate_column_names(count)

    def get_data_handle(self):
        return DataHandler(self)

    def prep_data_load(self):
        """ Resets the file index, and reads off the header row, if needed """
        self._handle.seek(0)
        if self.has_header_row():
            self._get_headers_from_handle(self._handle)

    def _get_headers_from_handle(self, handle):
        handle.seek(0)
        values = self._next(self._get_csv_reader(handle))
        unique = self.make_unique_columns(values)

        return unique

    def _get_csv_reader(self, handle):
        # From https://docs.python.org/2/library/csv.html
        def utf_8_encoder(unicode_csv_data):
            for line in unicode_csv_data:
                yield line.encode('utf-8')

        if six.PY2:
            return csv.reader(utf_8_encoder(handle),
                              delimiter=str(self._delimiter))
        elif six.PY3:
            return csv.reader(handle, delimiter=str(self._delimiter))

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

    def clear_column_types(self):
        """
        Clears the column type cache.  Mainly for unit testing.
        """
        self._column_types = None

    def column_types(self):
        """
        Does a best-guess at column types.  Currently supports detection of
        int, float and text types.  This list could expand in the future!

        Implementation of the column types is up to the DB engine.  Since the
        types can change, there should be a fallback for new types (probably
        text)

        Currently the preferred matches goes int -> float -> text

        Resets the handle to position 0.
        """
        MAX_COLUMN_TYPE_GUESS = 100

        if self._column_types:
            return self._column_types
        if not self._handle:
            raise Exception("No handle to read from")

        self._handle.seek(0)

        values = []
        # Skip the header if we have one:
        if self.has_header_row():
            self.next()

        row_num = 0
        for row in self:
            row_num += 1
            if row_num > MAX_COLUMN_TYPE_GUESS:
                break
            self._guess_column_types_by_row(row, values)

        names = self.column_names()
        if values == []:
            for name in names:
                values.append({"type": "text", "max": 100})

        if len(values) < len(names):
            for i in range(len(names)-len(values)):
                values.append({"type": "text", "max": 100})

        self._handle.seek(0)
        self._column_types = values
        return values

    def _guess_column_types_by_row(self, row, values):
        # Fallback for missing column definition
        def _cv(i, v):
            if i < len(v):
                return v[i]
            return {"type": None}

        # utility for the text type definition
        def _get_text_value(value):
            return {"type": "text", "max": len(value)}

        # utility for the float type definition
        def _get_float_value(value):
            return {"type": "float"}

        # utility for the int type definition
        def _get_int_value(value):
            return {"type": "int"}

        # the CSV module always returns strings... so we do our own attempt
        def _is_int(value):
            try:
                int(value)
                return True
            except Exception:
                return False

        def _is_float(value):
            try:
                float(value)
                return True
            except Exception:
                return False

        for i in range(0, len(row)):
            value = _cv(i, values)
            if "text" == value["type"]:
                # Just need to see if the length is longer:
                if len(row[i]) > value["max"]:
                    values[i] = _get_text_value(row[i])
            elif "float" == value["type"]:
                # We only need to check if this is a string.  If it's a float
                # or an int, we're already set
                if not _is_int(row[i]) and not _is_float(row[i]):
                    values[i] = _get_text_value(row[i])
            elif "int" == value["type"]:
                # Check both fallbacks
                if not _is_int(row[i]):
                    if _is_float(row[i]):
                        values[i] = _get_float_value(row[i])
                    else:
                        values[i] = _get_text_value(row[i])
            else:
                # Check all three...
                if _is_int(row[i]):
                    values.append(_get_int_value(row[i]))
                elif _is_float(row[i]):
                    values.append(_get_float_value(row[i]))
                else:
                    values.append(_get_text_value(row[i]))

    def has_header_row(self, value=None):
        if value is not None:
            self._has_header = value
        return self._has_header

    def column_names(self):
        return self._column_names

    def delimiter(self, value=None):
        if value is not None:
            self._delimiter = value
        return self._delimiter

    # To handle python 2/3 differences
    def _next(self, handle):
        if six.PY2:
            return [unicode(cell, 'utf-8') for cell in handle.next()]
        elif six.PY3:
            return next(handle)

    # To make this iterable - intended to make it so we can be somewhat
    # low-memory, even on large files
    def __iter__(self):
        return self

    def next(self):
        if six.PY2:
            delimiter = self.delimiter().encode("ascii")
        elif six.PY3:
            delimiter = self.delimiter()
        reader = self._get_csv_reader(self._handle)
        return self._next(reader)

    # Python 3 version of next
    __next__ = next


class DataHandler(object):
    """
    To track errors in iteration, look for a string instead of list.
    """
    def __init__(self, parser):
        self._parser = parser
        self._columns = parser.column_types()
        parser.prep_data_load()

    def __iter__(self):
        return self

    def next(self):
        # We try to type the data, but if it isn't correct we just add the
        # raw strings, and hope a later step can sort it out.
        semi_typed = []
        raw = self._parser.next()

        try:
            for i in range(0, len(self._columns)):
                col_type = self._columns[i]["type"]

                if len(raw) <= i:
                    # Make the data square!
                    semi_typed.append(None)
                else:
                    try:
                        value = raw[i]
                        if "int" == col_type:
                            semi_typed.append(int(value))
                        elif "float" == col_type:
                            semi_typed.append(float(value))
                        else:
                            semi_typed.append(value)
                    except Exception as ex:
                        semi_typed.append(value)
            return semi_typed

        except Exception as ex:
            return str(ex)

    __next__ = next


def open_encoded(filename, mode):
    handle = open(filename, "rb")
    sample = handle.read()

    encoding = chardet.detect(sample)["encoding"]
    handle.close()

    return io.open(filename, mode=mode, encoding=encoding)
