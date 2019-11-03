import bs4
import re


def Capture_Hashtags(text):
    
    #task 1: Clean up HTML (mainly new lines)
    soup = bs4.BeautifulSoup(text, 'lxml')
    text = soup.get_text()
    text = text.replace('\n',' ')
    text = text.replace('\r','.')
    
    #task 4: remove links
    text = re.sub('https?://[A-Za-z0-9./~]+','', text)
    hashtags = re.findall(r'#(\w+)',text)
    return hashtags
