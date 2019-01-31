import csv
import logging

from argparse import ArgumentParser
from configparser import ConfigParser
from indeed import IndeedClient
from os import path, makedirs
from pymongo import MongoClient

from topofthepile.email_client import EmailClient
from topofthepile.job import MachineLearningJob
from topofthepile.job_search import IndeedJobSearch


def scrape_cities():
    """
    Get list of cities in the United States with a population of at least 15,000
    :return: Cities
    """
    cities = []
    cities_file_path = './submodule/world-cities/data/world-cities.csv'
    cache_folder_path = './cache/'
    cities_cache_filename = 'world-cities.csv'

    if not path.exists(cache_folder_path):
        makedirs(cache_folder_path)
    if not path.exists(cache_folder_path + cities_cache_filename):
        # Read raw city data
        with open(cities_file_path) as file:
            reader = csv.reader(file)
            for row in reader:
                if row[1] == 'United States':
                    cities.append(row[0] + ', ' + row[2])

        # Cache formatted data
        with open(cache_folder_path + cities_cache_filename, 'w+') as file:
            writer = csv.writer(file)
            for city in cities:
                writer.writerow([city])
    else:
        # Read from cache
        with open(cache_folder_path + cities_cache_filename) as file:
            reader = csv.reader(file)
            cities = [row[0] for row in reader]

    return cities


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

    locations = args.locations if args.locations else scrape_cities()
    ml_job = MachineLearningJob(database, logger)
    indeed_job_search = IndeedJobSearch(indeed_api, logger)

    for location in locations:
        newest_job = ml_job.get_newest_job(location, indeed_job_search.NAME)
        newest_date = newest_job['date'] if newest_job else None
        new_jobs = indeed_job_search.get_new_jobs(ml_job.TITLE, location, newest_date)

        # Log
        sample_max_city_name_length = 35
        debug_log_string = 'Scraped location {:<' + str(sample_max_city_name_length) + '} found {:>3} jobs.'
        logger.debug(debug_log_string.format(location, len(new_jobs)))

        if new_jobs:
            ml_job.add_jobs(new_jobs, location, indeed_job_search.NAME)

    ml_job.process_all_jobs()
    email_jobs = ml_job.get_jobs_for_email()
    if email_jobs:
        email_client.email_jobs(
            email_jobs,
            config_parser['EMAIL']['FromAddress'],
            config_parser['EMAIL']['ToAddress'],
            config_parser['EMAIL']['UseSSL'])
        try:
            ml_job.set_emailed_jobs(email_jobs)
        except Exception as error:
            logger.error('Coudn\'t update database with emails sent')
            raise error


if __name__ == '__main__':
    run()
