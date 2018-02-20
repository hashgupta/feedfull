#File not needed

import requests
from warcio.archiveiterator import ArchiveIterator

def print_records(url):
    resp = requests.get(url, stream=True)

    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.rec_type == 'warcinfo':
            print(record.raw_stream.read())

        elif record.rec_type == 'response':
            if record.http_headers:
                print(record.http_headers)
                if record.http_headers.get_header('Content-Type') == 'text/html':
                    print(record.http_headers + ("\n" * 4))

# WARC
print_records('https://archive.org/download/ExampleArcAndWarcFiles/IAH-20080430204825-00000-blackbook.warc.gz')
