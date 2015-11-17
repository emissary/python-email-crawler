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

google_adurl_regex = re.compile('adurl=(.*?)"')
google_url_regex = re.compile('url\?q=(.*?)&amp;sa=')
email_regex = re.compile('([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4})', re.IGNORECASE)
url_regex = re.compile('<a\s.*?href=[\'"](.*?)[\'"].*?>')
# Below url_regex will run into 'Castrophic Backtracking'!
# http://stackoverflow.com/questions/8010005/python-re-infinite-execution
# url_regex = re.compile('<a\s(?:.*?\s)*?href=[\'"](.*?)[\'"].*?>')

# Maximum number of search results to start the crawl
MAX_SEARCH_RESULTS = 200

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

  for d in directory_domains:
    keywords = keywords + ' -site:' + d

  for page_index in range(0, MAX_SEARCH_RESULTS, 10):

    query = {'q': keywords}
    url = 'http://www.google.com/search?' + urllib.urlencode(query) + '&start=' + str(page_index)
    data = retrieve_html(url)
#    print("data: \n%s" % data)
    for url in google_url_regex.findall(data):
      db.enqueue(url)
    for url in google_adurl_regex.findall(data):
      db.enqueue(url)

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
  req.add_header('cookie', """TZ=480; GOOGAPPUID=668; GoogleAccountsLocale_session=en; AdsUserLocale=en_US; __utma=1.394066841.1407358518.1415827446.1415827446.1; __utmc=1; S_adsense3-ui=s0ZLgOcog5RaSETHLJZpoQ; PREF=ID=1111111111111111:LD=en:CR=2:TM=1416615749:LM=1434746594:GM=1:S=x1isPa8cnUjlc2W_;
  S=izeitgeist-ad-metrics=PmgbiemVSEs:quotestreamer=5IWJZk4Vlj44eOurnqEuzg:analytics-realtime-frontend=X1vAzTJD2C_bbi8t5GzzeQ:adwords-usermgmt=HUqWc0qVwhJ7BZuJO5htTA:adwords-campaignmgmt=Os5qvyUVFAI2aAE7kjjQaw:adwords-common-ui=V_icxuG5rjcbV-xq1iVOxg:adwords-navi=4jB-FRquk6Oic-o8_q8qnw:adwords-kwoptimization=bqSO0xdkmDCPnvf89kkWsQ:photos_html=TiRUmTS92HrqENqwXneGSg:grandcentral=WBfJ08PaJJtVfc91dDmbew:cloudsearch=Hw_w9PiNzvvcraZsWRXizA; GMAIL_RTT=156; HSID=Ag6g8UuxleWJ6Ln07;
  SSID=ApO6KLdEtjnDLJ718; APISID=69P5LE8WWDBWLJ-J/ACFpDjkhE3fMwfgXb; SAPISID=z2jb76E6GXaE7Iy5/A2ZXL7XP7bThkCp4m; S=adwords-usermgmt=mKyGOHhWsCb6ePDJw6XS6A:adwords-campaignmgmt=j3WEUY85TVcHXrKnsbxo7g:adwords-common-ui=RiaYfCGdn7Q91cI2km6zIg:adwords-navi=yoMJ3re_rW26iNO8q394Vw:videobuying=gWAXWMfpbKAS4H_RHpEo9Q:adwords-kwoptimization=QlG6-2NrLrDVzb2c93Kqrw:grandcentral=zFFpW8fOaIij1ElXHoimnw; _ga=GA1.1.394066841.1407358518;
  SID=DQAAADQCAABWjswSa4M6uc4KxaE-hmDiRzLBdL7GMSEiFQ6PlJPMEpSOxG08XvCpF0Sdul9xg-hGO30CimalLijmcRhPpG7pKOKB9XL7b0NGVvaKhsCuALeF0phoziKBgs3IdI5YZ51y9HDGudV5RNRwq3PfI19eegzlvO-qx6V2dxYjU8pWigyhMQ9kY4nZldpXv10J8gcWVpkDaSmXpsGa8G8a3sRNMcDdFKgHDxO9m5-g_ANJIne7rCIdKJCERUXhSEYe7gV-ldf8NDmA_Ksfy5Pny5SWP3oV3LgStrTqyMJtwYYujt2J9-qdkkGvoTlKnK61Ereju49mNMqQVFq2YCq3w30RPZbcy0O_HJA2kEIJCU-ne0UoAQe-6ITHPww5ZDi28wmdCZzCqkHOe-uDY6zlHU16V5nntuQuz9IUTZjOJ2z2OcYq0RB-9biZCn-DzT7-4P2qOKw_ShC8hOwugcS1uJCitZAik_q3a-hN-GW3MUxk0GZ_-tsODeS3j1M83RTH6mw8IeWYl9f5laC66Terl923vdRIeJhJhwbeiRVtWk3iWt_YwNa7L5n1sCM01YlQEWKUYbdx1lsnvDecAb6HATvptjWKnThEZDUuOB767Wh0JppQoWqFoFjELfOxVKlsf332fxgyke5EowOo9OBbY35lRHMLvq7CU-vr3VYOAW1X4Wp7VDFFDHZZC2yOqVo96Q-vFMXCFCAGN9xrkx6JDfOYLWlq5vvuym3hMq6We9JJRoaRbHJbKHQhKcNfmweHsqU;
  NID=73=p_J06bzimh9VPmcN_jt-8VRHiUrWnhet_5ttTRGnQnyETWsyJEF_jUw3fddJuVi8KxxeiK2hBZCPwzT108ShEnezlecY3CnrnGzDGQJIzjk3CLmqaT0RQX3oKZhlopDGRNeSn5_6ckbJ9Jsw6fWemtmbys4-IEWZzudybGHbzrxoCtSkj7VYnhJ24Q6xodZ6uMVp2Z5yGcCH2LQUGXPGiydqYIey8zq5mLMWHWOm3BmVgjd817otQkrwn_-1ou0KTah3rbhBNaiL5T97vzyGoNw8TX7leXMPf530vbljyrkoC8_qx8cAYxoxDKWYfWgGNhGF9ldabyEIQo42JorbGS00qWNaVOFZiqeKVSghnM9_vdqidx_Qspm5ww_nvaPObiZ_qgNjj5tEKluwiIBgoz21HTDr8yxZ8g;
  GOOGLE_ABUSE_EXEMPTION=ID=bae7753416360280:TM=1447712532:C=c:IP=86.5.242.71-:S=APGng0sWW4tfCWNonWtyROmYwVKbK13IqQ; DV=kq3lKI4IBRMWHttpngnuh9YsCckkogI""")

  request = None
  status = 0
  try:
    logger.info("Crawling %s" % url)
    request = urllib2.urlopen(req)
  except urllib2.URLError, e:
    logger.error("Exception at url: %s\n%s" % (url, e))
  except urllib2.HTTPError, e:
    status = e.code
  except Exception, e:
    return
  if status == 0:
    status = 200

  try:
    data = request.read()
  except Exception, e:
    return

  return str(data)


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
      # Crawl them right away!
      # Enqueue them too
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

