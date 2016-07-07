import os
import logging
import sys
from datetime import datetime
from scraper import run_scraper
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig()

sched_interval_seconds = os.getenv('SCRAPER_SCHED_INTERVAL_SECONDS')
if sched_interval_seconds is None:
    sched_interval_seconds = (60*60*2) # run every 2 hours
else:
    sched_interval_seconds = int(sched_interval_seconds)

onetime = os.getenv('SCRAPER_SCHED_ONETIME') or 'false'
# archive = os.getenv('SCRAPER_SCHED_ARCHIVE_ENABLED') or 'true'


# @timeout(seconds=sched_interval_seconds)
def timeout_runner():
    time_start = datetime.utcnow()
    print('----[ timeout_runner, begin. time: {}'.format(str(time_start)))
    try:
        run_scraper()
    except Exception:
        print "====[ Exception in scraper_sched.timeout_runner: ", sys.exc_info()[0]

    time_end = datetime.utcnow()
    print('----[ timeout_runner, end. duration: {} time: {}'.format(str(time_end - time_start), time_end))

if __name__ == '__main__':
    print('running one-time: '+onetime)
    if onetime == 'true':
        print('----[ performing one-time single scrape')
        run_scraper()
    else:
        print('----[ performing recurrent scrape every ' + str(sched_interval_seconds) + ' seconds')
        timeout_runner()
        scheduler = BlockingScheduler()
        scheduler.add_job(timeout_runner, 'interval', seconds=sched_interval_seconds)
        scheduler.start()
