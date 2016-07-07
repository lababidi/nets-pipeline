import os
import re
import glob
import json
import logging
import pattern.web
import pages_scrape
from goose import Goose
from elasticsearch import Elasticsearch
from ConfigParser import ConfigParser
from multiprocessing import Pool
from datetime import datetime
import parsedatetime as pdt

# Initialize Logger
logger = None


def scrape_func(website, lang, address, COLL, index_auth, db_user, db_pass, db_host=None):
    """
    Function to scrape various RSS feeds.

    Parameters
    ----------

    website: String
            Nickname for the RSS feed being scraped.

    address: String
                Address for the RSS feed to scrape.

    COLL: String
            Collection within MongoDB that holds the scraped data.

    db_auth: String.
                MongoDB database that should be used for user authentication.

    db_user: String.
                Username for MongoDB authentication.

    db_user: String.
                Password for MongoDB authentication.
    """
    # Setup the database

    if db_host:
        connection = Elasticsearch(
            [db_host],
            http_auth=(db_user, db_pass)
        )
    else:
        connection = Elasticsearch(
            [{'host': 'localhost', 'port': 9200}],
            http_auth=(db_user, db_pass)
        )

    # indices = connection.indices.get_aliases().keys()
    # Scrape the RSS feed
    results = get_rss(address, website)

    # Pursue each link in the feed
    if results:
        parse_results(results, website, lang, connection, index_auth)

    logger.info('Scrape of {} finished'.format(website))


def get_rss(address, website):
    """
    Function to parse an RSS feed and extract the relevant links.

    Parameters
    ----------

    address: String.
                Address for the RSS feed to scrape.

    website: String.
                Nickname for the RSS feed being scraped.

    Returns
    -------

    results : pattern.web.Results.
                Object containing data on the parsed RSS feed. Each item
                represents a unique entry in the RSS feed and contains relevant
                information such as the URL and title of the story.

    """
    try:
        results = pattern.web.Newsfeed().search(address, count=100,
                                                cached=False, timeout=30)
        logger.debug('There are {} results from {}'.format(len(results),
                                                           website))
    except Exception, e:
        print 'There was an error. Check the log file for more information.'
        logger.warning('Problem fetching RSS feed for {}. {}'.format(address,
                                                                     e))
        results = None

    return results


def parse_results(rss_results, website, lang, connection, auth_index):
    """
    Function to parse the links drawn from an RSS feed.

    Parameters
    ----------

    rss_results: pattern.web.Results.
                    Object containing data on the parsed RSS feed. Each item
                    represents a unique entry in the RSS feed and contains
                    relevant information such as the URL and title of the
                    story.

    website: String.
                Nickname for the RSS feed being scraped.

    connection: Elastic search conection
                Connection used to read and write to db
    """
    if lang == 'english':
        goose_extractor = Goose({'use_meta_language': False,
                                 'target_language': 'en',
                                 'enable_image_fetching': False})
    elif lang == 'arabic':
        from goose.text import StopWordsArabic

        goose_extractor = Goose({'stopwords_class': StopWordsArabic,
                                 'enable_image_fetching': False})
    else:
        print(lang)

    cal = pdt.Calendar()
    for result in rss_results:

        page_url = _convert_url(result.url, website)

        in_database = _check_url(page_url, connection, auth_index)

        if not in_database:
            try:
                text, meta = pages_scrape.scrape(page_url, goose_extractor)
                text = text.encode('utf-8')
            except TypeError:
                logger.warning('Problem obtaining text from URL: {}'.format(page_url))
                text = ''
        else:
            logger.debug('Result from {} already in database'.format(page_url))
            text = ''

        if text:
            try:
                time_struct, parse_status = cal.parse(result.date)
                date_parsed = datetime(*time_struct[:6]).isoformat()
            except (Exception, TypeError, ValueError, KeyError, IndexError, OverflowError, AttributeError):
                date_parsed = None

            cleaned_text = _clean_text(text, website)
            response = connection.index(index=auth_index, doc_type=auth_index, body={
                "content": unicode(cleaned_text, 'utf-8'),
                "title": result.title,
                "url": result.url,
                "date_published_original": result.date,
                "date_published": date_parsed,
                "date_collected": datetime.now().isoformat(),
                "source": website,
                "language": lang
            })

            logger.info(response)

            #entry_id = mongo_connection.add_entry(db_collection, cleaned_text,
            #                                      result.title, result.url,
            #                                      result.date, website, lang)
            #if entry_id:
            #    try:
            #        logger.info('Added entry from {} with id {}'.format(page_url,
            #                                                            entry_id))
            #    except UnicodeDecodeError:
            #        logger.info('Added entry from {}. Unicode error for id'.format(result.url))


def _check_url(url, connection, auth_index):
    """
    Private function to check if a URL appears in the database.

    Parameters
    ----------

    url: String.
            URL for the news stories to be scraped.

    connection: elastic search connection

    Returns
    -------

    found: Boolean.
            Indicates whether or not a URL was found in the database.
    """
    response = connection.search(index=auth_index, doc_type=auth_index, body={
        "query":
            {
                "match_phrase": {
                    "url": url
                }
            }
        }, size=0, terminate_after=1, ignore_unavailable=True)

    return response["hits"]["total"] > 0


def _convert_url(url, website):
    """
    Private function to clean a given page URL.

    Parameters
    ----------

    url: String.
            URL for the news stories to be scraped.

    website: String.
                Nickname for the RSS feed being scraped.

    Returns
    -------

    page_url: String.
                Cleaned and unicode converted page URL.
    """

    if website == 'xinhua':
        page_url = url.replace('"', '')
        page_url = page_url.encode('ascii')
    elif website == 'upi':
        page_url = url.encode('ascii')
    elif website == 'zaman':
        # Find the weird thing. They tend to be ap or reuters, but generalized
        # just in case
        com = url.find('.com')
        slash = url[com + 4:].find('/')
        replaced_url = url.replace(url[com + 4:com + slash + 4], '')
        split = replaced_url.split('/')
        # This is nasty and hackish but it gets the jobs done.
        page_url = '/'.join(['/'.join(split[0:3]), 'world_' + split[-1]])
    else:
        page_url = url.encode('utf-8')

    return page_url


def _clean_text(text, website):
    """
    Private function to clean some of the cruft from the content pulled from
    various sources.

    Parameters
    ----------

    text: String.
            Dirty text.

    website: String.
                Nickname for the RSS feed being scraped.

    Returns
    -------

    text: String.
            Less dirty text.
    """
    site_list = ['menafn_algeria', 'menafn_bahrain', 'menafn_egypt',
                 'menafn_iraq', 'menafn_jordan', 'menafn_kuwait',
                 'menafn_lebanon', 'menafn_morocco', 'menafn_oman',
                 'menafn_palestine', 'menafn_qatar', 'menafn_saudi',
                 'menafn_syria', 'menafn_tunisia', 'menafn_turkey',
                 'menafn_uae', 'menafn_yemen']

    if website == 'bbc':
        text = text.replace(
            "This page is best viewed in an up-to-date web browser with style sheets (CSS) "
            "enabled. While you will be able to view the content of this page in your current "
            "browser, you will not be able to get the full visual experience. Please consider "
            "upgrading your browser software or enabling style sheets (CSS) if you are able to do "
            "so.",
            '')
    if website == 'almonitor':
        text = re.sub("^.*?\(photo by REUTERS.*?\)", "", text)
    if website in site_list:
        text = re.sub("^\(.*?MENAFN.*?\)", "", text)
    elif website == 'upi':
        text = text.replace(
            "Since 1907, United Press International (UPI) has been a leading provider of critical "
            "information to media outlets, businesses, governments and researchers worldwide. UPI "
            "is a global operation with offices in Beirut, Hong Kong, London, Santiago, Seoul and "
            "Tokyo. Our headquarters is located in downtown Washington, DC, surrounded by major "
            "international policy-making governmental and non-governmental organizations. UPI "
            "licenses content directly to print outlets, online media and institutions of all "
            "types. In addition, UPI's distribution partners provide our content to thousands of "
            "businesses, policy groups and academic institutions worldwide. Our audience consists "
            "of millions of decision-makers who depend on UPI's insightful and analytical stories "
            "to make better business or policy decisions. In the year of our 107th anniversary, "
            "our company strives to continue being a leading and trusted source for news, "
            "analysis and insight for readers around the world.",
            '')

    text = text.replace('\n', ' ')

    return text


def call_scrape_func(siteList, db_collection, pool_size, db_index, db_user,
                     db_pass, db_host=None):
    """
    Helper function to iterate over a list of RSS feeds and scrape each.

    Parameters
    ----------

    siteList: dictionary
                Dictionary of sites, with a nickname as the key and RSS URL
                as the value.

    db_collection : collection
                    Mongo collection to put stories

    pool_size : int
                Number of processes to distribute work
    """
    pool = Pool(pool_size)
    results = [pool.apply_async(scrape_func, (address, lang, website,
                                              db_collection, db_index, db_user,
                                              db_pass, db_host))
               for address, (website, lang) in siteList.iteritems()]
    [r.get(9999999) for r in results]
    pool.terminate()
    logger.info('Completed full scrape.')


def _parse_config(parser):
    try:
        if 'Auth' in parser.sections():
            auth_index = parser.get('Auth', 'auth_index')
            auth_user = parser.get('Auth', 'auth_user')
            auth_pass = parser.get('Auth', 'auth_pass')
            db_host = parser.get('Auth', 'db_host')
        else:
            # Try env vars too
            vcap_json = os.getenv('VCAP_SERVICES')
            if vcap_json != None:
                vcap = json.loads(vcap_json)
                auth_index = vcap['p-elasticsearch'][0]['credentials']['index']
                auth_user = vcap['p-elasticsearch'][0]['credentials']['username']
                auth_pass = vcap['p-elasticsearch'][0]['credentials']['password']
                db_host = str(vcap['p-elasticsearch'][0]['credentials']['host'])+':'+str(vcap['p-elasticsearch'][0]['credentials']['port'])
            else:
                db_host = '127.0.0.1:9200'
                auth_index = 'article'
                auth_user = ''
                auth_pass = ''

        log_dir = parser.get('Logging', 'log_file')
        print('logging to '+log_dir)
        log_level = parser.get('Logging', 'level')
        collection = parser.get('Database', 'collection_list')
        whitelist = parser.get('URLS', 'file')
        sources = parser.get('URLS', 'sources').split(',')
        pool_size = int(parser.get('Processes', 'pool_size'))
        return collection, whitelist, sources, pool_size, log_dir, log_level, auth_index, auth_user, \
               auth_pass, db_host
    except Exception, e:
        print 'Problem parsing config file. {}'.format(e)


def parse_config():
    """Function to parse the config file."""
    config_file = glob.glob('config.ini')
    parser = ConfigParser()
    if config_file:
        parser.read(config_file)
    else:
        cwd = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(cwd, 'default_config.ini')
        parser.read(config_file)
    return _parse_config(parser)


def run_scraper():
    global logger
    # Get the info from the cocfigcf
    db_collection, whitelist_file, sources, pool_size, log_dir, log_level, auth_index, auth_user, auth_pass, \
    db_host = parse_config()
    print 'Scraper connecting to db at ' + auth_index + ' with username: ' + auth_user + ' and password: ' + auth_pass + \
          ' and host: ' + db_host

    # Setup the logging
    logger = logging.getLogger('scraper_log')
    if log_level == 'info':
        logger.setLevel(logging.INFO)
    elif log_level == 'warning':
        logger.setLevel(logging.WARNING)
    elif log_level == 'debug':
        logger.setLevel(logging.DEBUG)

    if log_dir:
        fh = logging.FileHandler(log_dir, 'a')
    else:
        fh = logging.FileHandler('scraping.log', 'a')
    formatter = logging.Formatter('%(levelname)s %(asctime)s: %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.info('Running in scheduled hourly mode')

    print 'Running. See log file for further information.'

    # Convert from CSV of URLs to a dictionary
    try:
        url_whitelist = open(whitelist_file, 'r').readlines()
        url_whitelist = [line.replace('\n', '').split(',') for line in
                         url_whitelist if line]
        # Filtering based on list of sources from the config file
        to_scrape = {listing[0]: [listing[1], listing[3]] for listing in
                     url_whitelist if listing[2] in sources}
    except IOError:
        print 'There was an error. Check the log file for more information.'
        logger.warning('Could not open URL whitelist file.')
        raise

    call_scrape_func(to_scrape, db_collection, pool_size, auth_index, auth_user,
                     auth_pass, db_host=db_host)
    logger.info('All done.')


if __name__ == '__main__':
    run_scraper()
