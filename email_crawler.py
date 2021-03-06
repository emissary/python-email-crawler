from settings import LOGGING
import logging, logging.config
import urllib, urllib2
import re, urlparse
import traceback
from database import CrawlerDb

# Debugging
# import pdb;pdb.set_trace()

# Logging
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("crawler_logger")

yelp_url_regex = re.compile('class="biz-name" href="/biz/(.*?)"')
email_regex = re.compile('([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4})', re.IGNORECASE)
yelp_page_regex = re.compile('<a.*?url=http%3A%2F%2F(.*?)[&%]')
url_regex = re.compile('<a\s.*?href=[\'"](.*?)[\'"].*?>')
# Below url_regex will run into 'Castrophic Backtracking'!
# http://stackoverflow.com/questions/8010005/python-re-infinite-execution
# url_regex = re.compile('<a\s(?:.*?\s)*?href=[\'"](.*?)[\'"].*?>')

# Maximum number of search results to start the crawl
MAX_SEARCH_RESULTS = 1000

EMAILS_FILENAME = 'data/emails.csv'
DOMAINS_FILENAME = 'data/domains.csv'

# Set up the database
db = CrawlerDb()
db.connect()

#dentist chicago -site:yelp.com -site:zocdoc.com -site:1800dentist.com -site:yellowpages.com -site:whitepages.com
directory_domains = [
    'yelp.co.uk',
    'yelp.com',
    'zocdoc.com',
    '1800dentist.com',
    'yellowpages.com',
    'whitepages.com',
    'angieslist.com',
  ]


def crawl(keywords):
  """
  This method will

  1) Google the keywords, and extract MAX_SEARCH_RESULTS
  2) For every result (aka website), crawl the website 2 levels deep.
    That is the homepage (level 1) and all it's links (level 2).
    But if level 1 has the email, then skip going to level 2.
  3) Store the html in /data/html/ and update the database of the crawled emails

  crawl(keywords):
    Extract Google search results and put all in database
    Process each search result, the webpage:
      Crawl webpage level 1, the homepage
      Crawl webpage level 2, a link away from the homepage
      Update all crawled page in database, with has_crawled = True immediately
      Store the HTML
  """
  logger.info("-"*40)
  logger.info("Keywords to Google for: %s" % keywords)
  logger.info("-"*40)

# Step 1: Crawl Google Page
# eg http://www.google.com/search?q=singapore+web+development&start=0
# Next page: https://www.google.com/search?q=singapore+web+development&start=10
# Google search results are paged with 10 urls each. There are also adurls

  for page_index in range(0, MAX_SEARCH_RESULTS, 10):

    url = 'http://www.yelp.com/search?find_desc=dentist&find_loc=' + keywords + '&start=' + str(page_index)
    data = retrieve_html(url)
#   print("data: \n%s" % data)
    for url in yelp_url_regex.findall(data):
      yelp_page = retrieve_html("http://www.yelp.com/biz/" + url)
      biz_url = yelp_page_regex.findall(yelp_page)
      if biz_url:
        db.enqueue('http://' + biz_url[0])

# Step 2: Crawl each of the search result
# We search till level 2 deep
  while (True):
    # Dequeue an uncrawled webpage from db
    uncrawled = db.dequeue()
    if (uncrawled == False):
      break
    email_set = find_emails_2_level_deep(uncrawled.url)
    if (len(email_set) > 0):
      db.crawled(uncrawled, ",".join(list(email_set)))
    else:
      db.crawled(uncrawled, None)

def retrieve_html(url):
  """
  Crawl a website, and returns the whole html as an ascii string.

  On any error, return.
  """
  req = urllib2.Request(url)
  req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36')
  req.add_header('cookie', """yuv=DqmZXeB_WzCQcdJ5Zt04YMkRXZJBNu-NDKNm19REIqxsDqBrh1ha6NhaCeI33zLCbacOvFFmX-13heEi0Yy7Bw3rpr8GI3ts; bse=e6364c25ee8b29582697564ccc1478d7; hl=en_US; __qca=P0-773647723-1445386639160; fd=0; fbm_97534753161=base_domain=.yelp.com; qntcst=D%2CT; bip-iad1=sticky_web82-r8-iad1; recentlocations=Manhattan%2C+NY%2C+USA%3B%3BMiami%2C+FL%2C+USA%3B%3BSan+Francisco%2C+CA%2C+USA%3B%3BChicago%2C+IL%2C+USA%3B%3BNottingham%2C+UK%3B%3BSan+Francisco%2C+CA+94105%2C+USA; location=%7B%22city%22%3A+%22Columbus%22%2C+%22zip%22%3A+%22%22%2C+%22country%22%3A+%22US%22%2C+%22address2%22%3A+%22%22%2C+%22address3%22%3A+%22%22%2C+%22state%22%3A+%22OH%22%2C+%22address1%22%3A+%22%22%2C+%22unformatted%22%3A+%22columbus%2C+OH%2C+USA%22%7D; __utmt=1; __utmt_domainTracker=1; _gat_www=1; __utma=165223479.439679013.1445386638.1447879481.1447892012.5; __utmb=165223479.4.10.1447892012; __utmc=165223479; __utmz=165223479.1445386638.1.1.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utmv=165223479.|4=account%20level=anon=1; _ga=GA1.2.525099218A22D41D; crtg_ypus=; fbsr_97534753161=hWijeut1ir_5GFdJID20G2X9zfkQlxEqtHllONlaBIQ.eyJhbGdvcml0aG0iOiJITUFDLVNIQTI1NiIsImNvZGUiOiJBUUJ1NFVsWEw0N0dFeDN6M3B4UWw0LTBvbHNvbjVoSVhaLUhhOVFvaE03cU5lVTBIbjNGWVhCWkZVZGlYNWxKR1hKRktlZHo0bGJ5ODA5Y1gyOG1keThzU0JzR0hVd3Rud3VseTk4NE5Kc3psRXpscWlFbmZYT3N2SU5XOHlGcUFMY0Y1QTZMbC1ncmd1T2thRmNhVTBwSVV2MHFESjctMkpwRjFuNWdWa0VOMW55OXpoS3RhZTNtdnFvMi1NajU0WkV5OWphUWZMbVRLdjU2ckFmVV9PR094cTYwMDFNZkVmTjhZTU1fMURZbU83RXpFSEtNOG9WemZuZ2dvNWhBWko3cjg0ZjNfTGljbU5RYmR5Z0lhblZreXVlRVVTTWtIZEphNHRTbWxzeFNGT202b1hFN3FZTGtVTzd2TDc5SVBfSzVpRFRhOTRwQlRiNk9ZbG8xMDhQRCIsImlzc3VlZF9hdCI6MTQ0Nzg5MjQ1NywidXNlcl9pZCI6IjEzNjA1OTA1In0""")

  request = None
  tries = 0
  status = 0
  while tries < 3:
    tries = tries + 1
    try:
      logger.info("Crawling %s" % url)
      request = urllib2.urlopen(req, timeout=5)
    except urllib2.URLError, e:
      logger.error("Exception at url: %s\n%s" % (url, e))
    except urllib2.HTTPError, e:
      status = e.code
    except Exception, e:
      continue
    if status == 0:
      status = 200

    try:
      data = request.read()
    except Exception, e:
      continue 

    return str(data)
  return ''


def find_emails_2_level_deep(url):
  """
  Find the email at level 1.
  If there is an email, good. Return that email
  Else, find in level 2. Store all results in database directly, and return None
  """
  html = retrieve_html(url)
  email_set = find_emails_in_html(html)

  if (len(email_set) > 0):
    # If there is a email, we stop at level 1.
    return email_set

  else:
    # No email at level 1. Crawl level 2
    logger.info('No email at level 1.. proceeding to crawl level 2')

    link_set = find_links_in_html_with_same_hostname(url, html)
    for link in link_set:
      print link
      if url not in link:
        continue
      # Crawl them right away!
      # Enqueue them too
      if 'ada.org' in link or '.edu' in link or '.gov' in link:
        continue
      html = retrieve_html(link)
      if (html == None):
        continue
      email_set = find_emails_in_html(html)
      db.enqueue(link, list(email_set))

    # We return an empty set
    return set()


def find_emails_in_html(html):
  if (html == None):
    return set()
  email_set = set()
  for email in email_regex.findall(html):
    email_set.add(email)
  return email_set


def find_links_in_html_with_same_hostname(url, html):
  """
  Find all the links with same hostname as url
  """
  if (html == None):
    return set()
  url = urlparse.urlparse(url)
  links = url_regex.findall(html)
  link_set = set()
  for link in links:
    if link == None:
      continue
    try:
      link = str(link)
      if link.startswith("/"):
        link_set.add('http://'+url.netloc+link)
      elif link.startswith("http") or link.startswith("https"):
        if (link.find(url.netloc)):
          link_set.add(link)
      elif link.startswith("#"):
        continue
      else:
        link_set.add(urlparse.urljoin(url.geturl(),link))
    except Exception, e:
      pass

  return link_set
locations = [
    #'dallas,tx,usa',
'manhattan,NY',
'philadelphia,PA',
'san+diego,CA',
'baltimore,MD',
'minneapolis,MN',
    ]

if __name__ == "__main__":
  import sys
  try:
    arg = sys.argv[1].lower()
    if (arg == '--emails') or (arg == '-e'):
      # Get all the emails and save in a CSV
      logger.info("="*40)
      logger.info("Processing...")
      emails = db.get_all_emails()
      logger.info("There are %d emails" % len(emails))
      file = open(EMAILS_FILENAME, "w+")
      file.writelines("\n".join(emails))
      file.close()
      logger.info("All emails saved to ./data/emails.csv")
      logger.info("="*40)
    elif (arg == '--domains') or (arg == '-d'):
      # Get all the domains and save in a CSV
      logger.info("="*40)
      logger.info("Processing...")
      domains = db.get_all_domains()
      logger.info("There are %d domains" % len(domains))
      file = open(DOMAINS_FILENAME, "w+")
      file.writelines("\n".join(domains))
      file.close()
      logger.info("All domains saved to ./data/domains.csv")
      logger.info("="*40)
    else:
      # Crawl the supplied keywords!
      crawl(arg)

  except KeyboardInterrupt:
    logger.error("Stopping (KeyboardInterrupt)")
    sys.exit()
  except Exception, e:
    logger.error("EXCEPTION: %s " % e)
    traceback.print_exc()

