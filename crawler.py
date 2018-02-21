import gevent
from gevent import pool
from gevent import monkey
monkey.patch_all()
import requests
from warcio.archiveiterator import ArchiveIterator
import zmq
import ujson
from bs4 import BeautifulSoup
import processor
import operator

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5555")

POOL = pool.Pool(10)

def word_count(str):
    counts = dict()
    words = str.split()

    for word in words:
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1

    return counts

def print_records(url):
    resp = requests.get(url, stream=True)
    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.rec_type == 'warcinfo':
            print(record.raw_stream.read())

        elif record.rec_type == 'response':
            if record.http_headers.get_header('Content-Type') == 'text/html':
                soup = BeautifulSoup(record.content_stream().read().decode("utf-8"))


                # Process record here, maybe spacy

                text = process(soup)


                counts = word_count(text)

                top_3_words = sorted(counts.items(), key=operator.itemgetter(1), reverse=True)[:2]

                node = record.rec_headers.get_header('WARC-Target-URI')

                outlinks = ",".join([link['href'] for link in soup.find_all('a', href=True)])

                msg = bytes(ujson.dumps({"Node":node,"Keywords":",".join(top_3_words), "Outlinks":outlinks, "Score":1.0}), "utf-8")

                socket.send(msg)
    

def start_crawl():
    with open("warc copy.txt", "r") as textfile:
        urls = textfile.readlines()
    for url in urls[0]:
        POOL.spawn(print_records, "https://commoncrawl.s3.amazonaws.com/"+url)
    POOL.join()


start_crawl()
