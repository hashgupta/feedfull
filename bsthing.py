import re
import urllib.request
from bs4 import BeautifulSoup
    
# html = urllib.request.urlopen('http://bgr.com/2014/10/15/google-android-5-0-lollipop-release/')
# soup = BeautifulSoup(html)
# data = soup.findAll(text=True)
    
def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element.encode('utf-8'))):
        return False
    return True

def trim(element):
    if element.isspace():
        return False
    return True
    
def inside_trim(element):
    return "".join(element.split()) + " "
    
# result = filter(visible, data)
# result = filter(trim, result)
# result = map(inside_trim, result)


    
print(list(result))

def process(soup):
    soup = BeautifulSoup(html)
    data = soup.findAll(text=True)
    result = filter(visible, data)
    result = filter(trim, result)
    result = map(inside_trim, result)
    print("".join(result).rstrip())