import boto3
import json
import mongo_connection
import os
import pymongo

from ConfigParser import ConfigParser
from datetime import datetime, timedelta
from pymongo import MongoClient
from subprocess import call
from mongo_csv_exporter import export_mongo_csv

#this method walks through the scraper collection from a given start date, and continues on a day by day basis until there are no more entries
#it stores each day's scrapings in a csv which is then uploaded to an S3 bucket, and progress status/data is logged in a collection entry
def archive_func(start_date, scraper_collection, archive_collection):
    start_date_plus24 = start_date + timedelta(hours=24)
    daily_stories = scraper_collection.find({'date_added':{'$gte': start_date, '$lt':start_date_plus24}})
    stories_remaining = scraper_collection.find({'date_added':{'$gte': start_date}})

    #while there are still stories to be processed in the scraper collection
    while stories_remaining.count() > 0:
        #get the scraped stories for this date if there are any, and write them out to csv
        if daily_stories.count() > 0:
            #clear the old logging information
            if archive_collection.count() > 0:
                archive_collection.remove({})

            #build the query object for the export
            query = { 'date_added': { '$gte':start_date, '$lt': start_date_plus24}}

            #create file path for csv
            cwd = os.path.abspath(os.path.dirname(__file__))
            start_date_str = start_date.strftime("%Y-%m-%d")
            file_path = os.path.join(cwd, 'scrape_'+start_date_str+'.txt')
            export_mongo_csv(scraper_collection, query,['_id','content','source','date','language','title','url','date_added'], file_path)


            #upload the csv to the s3 bucket
            vcap_json = os.getenv('VCAP_SERVICES')
            if vcap_json != None:
                vcap = json.loads(vcap_json)
                vcap_bucket = vcap['aws-s3'][0]['credentials']['bucket']
                os.environ["AWS_ACCESS_KEY_ID"] = vcap['aws-s3'][0]['credentials']['access_key_id']
                os.environ["AWS_SECRET_ACCESS_KEY"] = vcap['aws-s3'][0]['credentials']['secret_access_key']
                os.environ["AWS_DEFAULT_REGION"] = vcap['aws-s3'][0]['credentials']['region']
            else:
                vcap_bucket = 'eventdb-test'

            s3 = boto3.resource('s3')
            bucket = s3.Bucket(vcap_bucket)
            data = open(file_path, 'rb')
            bucket.put_object(Key='scrape_'+start_date_str+'.txt', Body=data)

            #update the last time the archiver was run as well as the id of the last story that was processed
            #and delete intermediary file
            deleteLocalFiles = os.getenv('SCRAPER_DELETE_INTERMEDIARY_ARCHIVES') or 'false'
            if deleteLocalFiles == 'true':
                os.remove("scrape_"+start_date_str+".txt")
            lastrun = datetime.utcnow()
            archive_collection.insert({'last_run':lastrun, 'last_story_processed':daily_stories[daily_stories.count()-1]['_id'] })

        #increment start and end time by 24 hours and see if there are anymore stories remaining
        start_date = start_date_plus24
        start_date_plus24 = start_date + timedelta(hours=24)
        daily_stories = scraper_collection.find({'date_added':{'$gte': start_date, '$lt':start_date_plus24}})
        stories_remaining = scraper_collection.find({'date_added':{'$gte': start_date}})

#main function that connects to the stories db in mongo and archives the most recent scrapings
def run_scraper_archiver():
    #connection = MongoClient()

    vcap_json = os.getenv('VCAP_SERVICES')
    if vcap_json != None:
        vcap = json.loads(vcap_json)
        auth_db = vcap['p-mongodb'][0]['credentials']['database']
        auth_user = vcap['p-mongodb'][0]['credentials']['username']
        auth_pass = vcap['p-mongodb'][0]['credentials']['password']
        db_host = str(vcap['p-mongodb'][0]['credentials']['host'])+':'+str(vcap['p-mongodb'][0]['credentials']['port'])
        connection = MongoClient(host=db_host)
        connection[auth_db].authenticate(auth_user, auth_pass)
        db = connection[auth_db]
    else:
        parser = ConfigParser()
        cwd = os.path.abspath(os.path.dirname(__file__))
        config_file = os.path.join(cwd, 'default_config.ini')
        parser.read(config_file)

        if 'Auth' in parser.sections():
            auth_db = parser.get('Auth', 'auth_db')
            auth_user = parser.get('Auth', 'auth_user')
            auth_pass = parser.get('Auth', 'auth_pass')
            db_host = parser.get('Auth', 'db_host')
            connection = MongoClient(host=db_host)
            connection[auth_db].authenticate(auth_user, auth_pass)
            db = connection[auth_db]
        else:
            connection = MongoClient()
            db = connection.event_scrape

    #db = connection.event_scrape
    archive_collection = db['stories_archive']
    scraper_collection = db['stories']

    #get the last entry in the archive collection
    if archive_collection.count() > 0:
        last_archive = archive_collection.find()
        last_processed_id = last_archive[0]['last_story_processed']
        last_processed_story = scraper_collection.find({'_id':last_processed_id})
        date = last_processed_story[0]['date_added']
        start_date = date.replace(hour=0,minute=0,second=0,microsecond=0)

        #we start re-processing from the last date found which seems redundant just incase the application crashed before
        # archiving all entries from the date given
        archive_func(start_date, scraper_collection, archive_collection)
    else:
        #there are no entries in stories_archive so go through the scraper day by day, and then archive on a day by day basis
        scraped_stories = scraper_collection.find().sort('date_added', 1)
        if scraped_stories.count() > 0:
            date = scraped_stories[0]['date_added']
            start_date = date.replace(hour=0,minute=0,second=0,microsecond=0)
            archive_func(start_date, scraper_collection, archive_collection)

    connection.close()
if __name__ == '__main__':
    run_scraper_archiver()
