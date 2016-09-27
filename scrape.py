import csv

import ipgetter
import requests
from os import path, makedirs

from dateutil import parser
from pymongo import DESCENDING


def _update_array_fields(model, current_values, new_field_values):
    """
    Update all array fields if they don't contain the new values
    :param model: DB Base Model
    :param current_values: Dictionary of current values for model
    :param new_field_values: Dictionary of new values that should be in arrays
    :return:
    """
    update_array_fields = {}
    for field, value in new_field_values.items():
        if value not in current_values[field]:
            update_array_fields[field] = value

    if update_array_fields:
        model.update_one({'_id': current_values['_id']}, {'$push': update_array_fields})


def _finish_processing(database, job):
    """
    Finish processing scraped jobs
    :param database: Database to update the job
    :param job: Job to continue processing
    :return:
    """
    html_posting = requests.get(job['url']).content
    database.jobs.update_one(
        {'_id': job['_id']},
        {'$set': {
            'html_posting': html_posting,
            'finished_processing': True}})


def _setup_new_job(job, location):
    job['search_location'] = [location]
    job['date'] = parser.parse(job['date']).timestamp()
    job['finished_processing'] = False
    job['email_sent'] = False

    return job


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


def scrape_indeed(database, indeed_client, logger, job_title, locations):
    """
    Scrape job data from indeed and save it to the database
    :param database: Database to save the indeed data
    :param indeed_client: Indeed API client
    :param logger: Logger to log activity
    :param job_title: Job title to search for
    :param locations: Job locations to search for
    :return:
    """
    max_indeed_limit = 25
    sample_max_city_name_length = 35
    debug_log_string = 'Scraped location {:<' + str(sample_max_city_name_length) + '} found {:>3} jobs.'
    indeed_params = {
        'q': job_title,
        'limit': max_indeed_limit,
        'latlong': 1,
        'sort': 'date',
        'userip': ipgetter.myip(),
        'useragent': 'Python'
    }

    for location in locations:
        # Using a dicts instead of a list will prevent from adding duplicates
        new_jobs = {}
        update_jobs = {}
        result_start = 0
        newest_job = database.jobs.find_one({'search_location': location}, sort=[('date', DESCENDING)])
        indeed_response = indeed_client.search(**indeed_params, l=location, start=result_start)
        jobs = indeed_response['results']
        total_jobs = indeed_response['totalResults']

        if not jobs:
            logger.debug(debug_log_string.format(location, len(jobs)))
        else:
            if not newest_job:
                # Set the first job
                    new_jobs[jobs[0]['jobkey']] = _setup_new_job(jobs[0], location)
                    new_jobs[jobs[0]['jobkey']]['email_sent'] = True
            else:
                while result_start < total_jobs and newest_job['date'] < parser.parse(jobs[0]['date']).timestamp():
                    for job in jobs:
                        found_job = database.jobs.find_one({'jobkey': job['jobkey']})
                        if found_job:
                            update_jobs[found_job['jobkey']] = found_job
                        else:
                            new_jobs[job['jobkey']] = _setup_new_job(job, location)

                    result_start += indeed_params['limit']
                    jobs = indeed_client.search(**indeed_params, l=location, start=result_start)['results']

            try:
                if new_jobs:
                    logger.debug(debug_log_string.format(location, len(new_jobs)))
                    database.jobs.insert_many(new_jobs.values())
                for job_key, update_job in update_jobs.items():
                    _update_array_fields(
                        database.jobs,
                        update_job,
                        {'search_location': location})
            except Exception as error:
                logger.error('Updating db for search_location {} scrape data failed: {}'.format(location, error))

    unprocessed_jobs = database.jobs.find({'finished_processing': False})
    total_jobs = unprocessed_jobs.count()
    for job_i, job in enumerate(unprocessed_jobs):
        logger.debug('Processing job {:>3}/{:<3}'.format(job_i + 1, total_jobs))
        _finish_processing(database, job)
