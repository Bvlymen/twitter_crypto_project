import bs4
import re


def tweet_character_cleaner(text):
    
    #task 1: Clean up HTML (mainly new lines)
    soup = bs4.BeautifulSoup(text, 'lxml')
    text = soup.get_text()
    text = text.replace('\n',' ')
    text = text.replace('\r','.')
    
    #task 2: Find if RT and remove
    text = re.sub(r'^rt @[A-Za-z0-9_]+:','', text)

    #task 3: Remove mentions
    text = re.sub(r'@[A-Za-z0-9_]+','',text)

    #task 4: remove links
    text = re.sub('https?://[A-Za-z0-9./~]+','', text)
    
     #special meanings
    #ampersand code
    text = re.sub(r'&amp;','@',text)
    #left arrow code
    text = re.sub(r'&lt;','<',text)
    #right arrow code
    text = re.sub(r'&gt;','>',text)
    #quote marks
    text = re.sub(r'&quot;',' quotesymbol ',text)
    
    return text
