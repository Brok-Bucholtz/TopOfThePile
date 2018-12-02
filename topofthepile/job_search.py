import abc
import ipgetter

from dateutil import parser
from pymongo import DESCENDING


class AbstractJobSearch(abc.ABC):
    @abc.abstractmethod
    def get_jobs(self, job_title, location):
        raise NotImplemented()


class IndeedJobSearch(AbstractJobSearch):
    @staticmethod
    def _setup_new_job(job, location):
        """
        Set default values for a new job
        :param job: The new job
        :param location: The location the job was found
        :return: The job with the default values added
        """
        job['search_location'] = [location]
        job['date'] = parser.parse(job['date']).timestamp()
        job['finished_processing'] = False
        job['email_sent'] = False

        return job

    def __init__(self, database, indeed_client, logger):
        self.database = database
        self.indeed_client = indeed_client
        self.logger = logger

    def get_jobs(self, job_title, location):
        max_jobs = 25
        sample_max_city_name_length = 35
        result_start = 0
        debug_log_string = 'Scraped location {:<' + str(sample_max_city_name_length) + '} found {:>3} jobs.'
        indeed_params = {
            'q': job_title,
            'limit': max_jobs,
            'latlong': 1,
            'sort': 'date',
            'userip': ipgetter.myip(),
            'useragent': 'Python'
        }

        # Using a dicts instead of a list will prevent from adding duplicates
        new_jobs = {}
        update_jobs = {}

        newest_job = self.database.jobs.find_one({'search_location': location}, sort=[('date', DESCENDING)])
        indeed_response = self.indeed_client.search(**indeed_params, l=location, start=result_start)
        jobs = indeed_response['results']
        total_jobs = indeed_response['totalResults']

        self.logger.debug(debug_log_string.format(location, len(jobs)))
        if jobs:
            if not newest_job:
                # Set the first job
                new_jobs[jobs[0]['jobkey']] = self._setup_new_job(jobs[0], location)
                new_jobs[jobs[0]['jobkey']]['email_sent'] = True
            else:
                while result_start < total_jobs and newest_job['date'] < parser.parse(
                        jobs[0]['date']).timestamp():
                    for job in jobs:
                        found_job = self.database.jobs.find_one({'jobkey': job['jobkey']})
                        if found_job:
                            update_jobs[found_job['jobkey']] = found_job
                        else:
                            new_jobs[job['jobkey']] = self._setup_new_job(job, location)

                    result_start += indeed_params['limit']
                    jobs = self.indeed_client.search(**indeed_params, l=location, start=result_start)['results']

        return new_jobs, update_jobs
