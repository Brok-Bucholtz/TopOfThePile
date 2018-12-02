import csv
import logging

from argparse import ArgumentParser
from configparser import ConfigParser
from indeed import IndeedClient
from os import path, makedirs
from pymongo import MongoClient

from topofthepile.email_client import EmailClient
from topofthepile.job_filter import MachineLearningJobFilter
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


def update_array_fields(model, current_values, new_field_values):
    """
    Update all array fields if they don't contain the new values
    :param model: DB Base Model
    :param current_values: Dictionary of current values for model
    :param new_field_values: Dictionary of new values that should be in arrays
    :return:
    """
    array_fields = {}
    for field, value in new_field_values.items():
        if value not in current_values[field]:
            array_fields[field] = value

    if array_fields:
        model.update_one({'_id': current_values['_id']}, {'$push': array_fields})


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
    indeed_job_search = IndeedJobSearch(database, indeed_api, logger)
    ml_job_filter = MachineLearningJobFilter(database, logger)

    for location in locations:
        new_jobs, update_jobs = indeed_job_search.get_jobs(job_title, location)

        database.jobs.insert_many(new_jobs.values())

        for job_key, update_job in update_jobs.items():
            update_array_fields(
                database.jobs,
                update_job,
                {'search_location': location})

    ml_job_filter.filter_jobs(job_title, database.jobs.find({'finished_processing': False}))

    found_jobs_query = {'finished_processing': True, 'email_sent': False}
    found_jobs = list(database.jobs.find(found_jobs_query))
    if found_jobs:
        plural = 's' if len(found_jobs) > 1 else ''
        html_message = '<html>{}</html>'.format(
            '<br \>'.join(['<a href="{}">{}</a>'.format(job['url'], job['jobtitle']) for job in found_jobs]))
        email_client.send(
            config_parser['EMAIL']['FromAddress'],
            config_parser['EMAIL']['ToAddress'],
            'Top of the Pile: Found {} Job{}'.format(len(found_jobs), plural),
            html_message,
            config_parser['EMAIL']['UseSSL'].lower() == 'true')
        try:
            database.jobs.update_many(found_jobs_query, {'$set': {'email_sent': True}})
        except Exception as error:
            logger.error('Coudn\'t update database with emails sent')
            raise error


if __name__ == '__main__':
    run()
