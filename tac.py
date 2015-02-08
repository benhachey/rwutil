#!/usr/bin/env python
"Document and corpus handling for TAC source data"
from xml.etree.cElementTree import iterparse
import os
import re
import sgmllib


ENC = 'utf8'


class Document(object):
    def __init__(self, id, text):
        assert isinstance(id, unicode)
        self.id = id
        assert isinstance(text, unicode)
        self.text = text

    def __str__(self):
        return 'Document<{}..>'.format(
            ' '.join(self.text.split())[:50].encode(ENC)
            )


DOC_START_TAG = '<DOC>'
DOC_END_TAG = '</DOC>'
class DocSplitter(object):
    def __init__(self, file):
        self.file = file
        self.doc = None

    def __iter__(self):
        o = gzip.open if self.file.endswith('.gz') else open
        for line in o(self.file):
            if line.strip() == DOC_START_TAG:
                self.doc = []
            if self.doc is not None:
                self.doc.append(line)
            if line.strip() == DOC_END_TAG:
                yield ''.join(self.doc)
                self.doc = None


TEXT_TO_EXTRACT = frozenset([
    'docid',
    'doctype',
    'datetime',
    'headline',
    'text',
])
ATTRS_TO_EXTRACT = {
    'doctype': frozenset(['source']),
}
class DocParser(sgmllib.SGMLParser):
    "Parse doc attributes from a DOC SGML string"
    def __init__(self):
        sgmllib.SGMLParser.__init__(self)
        self.attrs = {}
        self._data = None

    def unknown_starttag(self, tag, attrs):
        if tag in TEXT_TO_EXTRACT:
            self._data = []
        self._process_attrs(tag, dict(attrs))

    def _process_attrs(self, tag, attrs):
        for name in ATTRS_TO_EXTRACT.get(tag, []):
            self.attrs[name] = attrs.get(name)

    def handle_data(self, data):
        if self._data is not None:
            self._data.append(data)

    def unknown_endtag(self, tag):
        if tag in TEXT_TO_EXTRACT:
            self.attrs[tag] = ''.join(self._data)
            self._data = None


class DocBuilder(object):
    "Builder for TAC documents"
    def __init__(self):
        self.head_re = re.compile(r'^.*<TEXT>', re.S)
        self.tag_re = re.compile(r'(?m)<[^>]*>')
        self.spacerepl = lambda mo: ' '*len(mo.group(0))

    def text(self, raw):
        """Return version of raw with space in place non body text.
        len(raw) == len(text(raw))."""
        assert isinstance(raw, unicode)
        text = self.head_re.sub(self.spacerepl, raw) # rm header
        text = self.tag_re.subn(self.spacerepl, text)[0] # rm sgml tags
        return text

    def __call__(self, string):
        p = DocParser()
        p.feed(string)
        p.close()
        doc = Document(
            p.attrs.get('docid'),
            self.text(string)
            )
        doc.raw = string
        doc.attrs = p.attrs
        return doc

class Query(object):
    def __init__(self, id, doc_id, start, end, name):
        assert isinstance(name, unicode)
        self.id = id
        self.doc_id = doc_id
        self.start = start
        self.end = end
        self.name = name

    def __str__(self):
        return u'Query<{}: {}>'.format(
            self.id,
            self.name
            ).encode(ENC)

# query xml element and attribute names
QUERY_ELEM = 'query'
QID_ATTR   = 'id'
DOCID_ELEM = 'docid'
START_ELEM = 'beg'
END_ELEM   = 'end'
NAME_ELEM  = 'name'
DEFAULT_OFFSET = 1
class QueryReader(object):
    def __init__(self, queries_file, offset=DEFAULT_OFFSET):
        """
        queries_file - file containing query set
        offset - integer to add to end offset (0 for <=2011, 1 for >=2012)
        """
        self.queries_file = queries_file
        self.offset = offset

    def __iter__(self):
        "Yield (qid, docid, start, end, name) tuples"
        for event, elem in iterparse(self.queries_file):
            if elem.tag == QUERY_ELEM:
                yield self._query(elem)

    def _query(self, query_elem):
        "Return (qid, docid, start, end, name) tuple"
        query_id = query_elem.get(QID_ATTR)
        d = {}
        for child in query_elem:
            d[child.tag] = child.text
        doc_id = d[DOCID_ELEM]
        start = int(d[START_ELEM])
        end = int(d[END_ELEM]) + self.offset
        name = unicode(d[NAME_ELEM])
        return Query(query_id, doc_id, start, end, name)


class CorpusReader(object):
    "Reader for TAC source data"
    def __init__(self, root):
        self.root = root
        self.build_doc = DocBuilder()

    def __iter__(self):
        "Yield document objects from corpus files under root"
        for f in self.files():
            for d in self.docs(f):
                yield d

    def files(self):
        "Walk root directory, yielding files"
        for root, dirs, files in os.walk(self.root):
            for f in files:
                yield os.path.join(root, f)

    def docs(self, file):
        "Yield document object for each sgml doc element in file"
        for doc_sgml_string in DocSplitter(file):
            yield self.build_doc(doc_sgml_string.decode(ENC))
