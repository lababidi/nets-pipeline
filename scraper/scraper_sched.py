import os
import logging
from datetime import datetime, timedelta
from scraper import run_scraper
from scraper_archiver import run_scraper_archiver
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig()

sched_interval_seconds = os.getenv('SCRAPER_SCHED_INTERVAL_SECONDS')
if sched_interval_seconds is None:
    sched_interval_seconds = (20*60)
else:
    sched_interval_seconds = int(sched_interval_seconds)

onetime = os.getenv('SCRAPER_SCHED_ONETIME') or 'false'
archive = os.getenv('SCRAPER_SCHED_ARCHIVE_ENABLED') or 'true'

#@timeout(seconds=sched_interval_seconds)
def timeout_runner():
    print('started scrape: '+str(datetime.utcnow()))
    run_scraper()
    #today = datetime.now()
    #today_plus_interval = today + timedelta(seconds=sched_interval_seconds)
    #if today.day != today_plus_interval.day:
    if archive == 'true':
        print('started archive: '+str(datetime.utcnow()))
        run_scraper_archiver()

if __name__ == '__main__':
    print('running one-time: '+onetime)
    if onetime == 'true':
        print('performing one-time single scrape')
        run_scraper()
        if archive == 'true':
            print('archiving single scrape is enabled')
            run_scraper_archiver()
    else:
        print('performing recurrent scrape every '+str(sched_interval_seconds)+' seconds')
        #run one time immediately
        timeout_runner()
        #true scheduling starts here
        scheduler = BlockingScheduler()
        scheduler.add_job(timeout_runner, 'interval', seconds=sched_interval_seconds)
        scheduler.start()
