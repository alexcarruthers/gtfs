import os
from codecs import iterdecode
from zipfile import ZipFile
import csv


class Record(object):
    """A Record is a single row in a CSV file"""

    def __init__(self, header, row):
        self.header = header
        self.row = row

    def to_dict(self):
        return dict([(fieldname, self.row[fieldindex] if fieldindex < len(self.row)
        else None) for fieldname, fieldindex in self.header.items()])

    def __repr__(self):
        return repr(self.to_dict())

    def __getitem__(self, name):
        try:
            return self.row[self.header[name]]
        except KeyError:
            return None


class Table(object):
    """A Table is a single CSV file"""

    def __init__(self, header, rows):
        # header is a dict of name->index
        self.header = dict(zip(header, range(len(header))))
        self.rows = rows

    def __repr__(self):
        return "<Table %s>" % self.header

    def __iter__(self):
        return self

    def next(self):
        return Record(self.header, self.rows.next())


class Feed(object):
    """A Feed is a collection of CSV files with headers, either zipped into an
    archive or loose in a folder"""

    def __init__(self, filename):
        self.filename = filename
        self.zf = None

        if not os.path.isdir(filename):
            self.zf = ZipFile(filename)

    def unicode_csv_reader(self, content_iter):
        reader = csv.reader([x.encode("utf-8") for x in iterdecode(content_iter,
                                                                   "utf-8")])
        for row in reader:
            yield [unicode(x, "utf-8") for x in row]
        return

    def get_rows(self, filename):
        if self.zf:
            try:
                contents = self.zf.read(filename)
            except KeyError:
                raise KeyError("%s is not present feed" % filename)
            return self.unicode_csv_reader(contents.split("\n"))
        else:
            return self.unicode_csv_reader(open(os.path.join(self.filename,
                                                             filename)))

    def get_table(self, filename):
        rows = self.get_rows(filename)
        return Table(rows.next(), rows)
