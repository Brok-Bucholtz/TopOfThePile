from argparse import ArgumentParser
from configparser import ConfigParser
import logging

from indeed import IndeedClient
from pymongo import MongoClient

from email_client import EmailClient
from scrape import scrape_indeed, scrape_cities


def run():
    app_name = 'top_of_the_pile'

    # Parse config file
    config_parser = ConfigParser()
    config_parser.read('config.ini')

    # Parse arguments
    arg_parser = ArgumentParser()
    arg_parser.add_argument('TaskType', help='The task to run', choices=['monitor_indeed'])
    arg_parser.add_argument('--locations', help='Specify the locations to use', nargs='+', type=str)
    arg_parser.add_argument('--verbose', help='Verbose Mode', action='store_true')
    args = arg_parser.parse_args()

    # Setup logging
    logger = logging.getLogger(app_name)
    log_format = logging.Formatter(
        '%(asctime)s [%(process)d][%(levelname)-8s-{:>7}]  --  %(message)s'.format(args.TaskType))
    console = logging.StreamHandler()
    console.setFormatter(log_format)
    logger.addHandler(console)

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Setup remote connections
    database = MongoClient(
        config_parser['DATABASE']['host'],
        int(config_parser['DATABASE']['port']))[config_parser['DATABASE']['Name']]
    indeed_api = IndeedClient(config_parser['INDEED']['PublisherNumber'])
    email_client = EmailClient(
        config_parser['EMAIL']['Host'],
        config_parser['EMAIL']['Port'],
        config_parser['EMAIL']['Username'],
        config_parser['EMAIL']['Password'])

    job_title = 'machine learning'
    locations = args.locations if args.locations else scrape_cities()
    scrape_indeed(database, indeed_api, logger, job_title, locations)
    found_jobs_query = {'finished_processing': True, 'email_sent': False}
    found_jobs = list(database.jobs.find(found_jobs_query))
    if found_jobs:
        plural = 's' if len(found_jobs) > 1 else ''
        html_message = '<html>{}</html>'.format(
            '<br \>'.join(['<a href="{}">{}</a>'.format(job['url'], job['jobtitle']) for job in found_jobs]))
        email_client.send(
            config_parser['EMAIL']['FromAddress'],
            config_parser['EMAIL']['ToAddress'],
            'Top of the Hat: Found {} Job{}'.format(len(found_jobs), plural),
            html_message,
            config_parser['EMAIL']['UseSSL'].lower() == 'true')
        try:
            database.jobs.update_many(found_jobs_query, {'$set': {'email_sent': True}})
        except Exception as error:
            logger.error('Coudn\'t update database with emails sent')
            raise error


if __name__ == '__main__':
    run()
