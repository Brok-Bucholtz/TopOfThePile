import abc
import ipgetter

from dateutil import parser


class AbstractJobSearch(abc.ABC):
    @property
    @abc.abstractmethod
    def NAME(self):
        raise NotImplemented()

    @abc.abstractmethod
    def get_new_jobs(self, job_title, location, min_date):
        raise NotImplemented()


class IndeedJobSearch(AbstractJobSearch):
    NAME = 'indeed'

    def __init__(self, indeed_client, logger):
        self.indeed_client = indeed_client
        self.logger = logger

    def get_new_jobs(self, job_title, location, min_date=None):
        jobs = []
        max_job_chunks = 25
        search_start = 0
        indeed_params = {
            'q': job_title,
            'limit': max_job_chunks,
            'latlong': 1,
            'sort': 'date',
            'userip': ipgetter.myip(),
            'useragent': 'Python'}

        indeed_response = self.indeed_client.search(**indeed_params, l=location, start=search_start)
        job_chunk = indeed_response['results']
        total_jobs = indeed_response['totalResults']

        # Log
        sample_max_city_name_length = 35
        debug_log_string = 'Scraped location {:<' + str(sample_max_city_name_length) + '} found {:>3} jobs.'
        self.logger.debug(debug_log_string.format(location, len(job_chunk)))

        if job_chunk:
            while search_start < total_jobs and (
                    not min_date or
                    min_date < parser.parse(job_chunk[0]['date']).timestamp()):
                jobs.extend(job_chunk)

                search_start += indeed_params['limit']
                job_chunk = self.indeed_client.search(**indeed_params, l=location, start=search_start)['results']

        return jobs
