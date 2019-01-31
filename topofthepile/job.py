import abc
import re
import requests

from bs4 import BeautifulSoup
from dateutil import parser
from pymongo import DESCENDING

from topofthepile.job_search import IndeedJobSearch


class AbstractJob(abc.ABC):
    @property
    @abc.abstractmethod
    def TITLE(self):
        raise NotImplemented()

    def __init__(self, jobs_collection, logger):
        self.jobs_collection = jobs_collection
        self.logger = logger

    def _setup_new_indeed_job(self, job, location, search_name):
        job['search_location'] = location
        job['search_name'] = search_name
        job['search_term'] = self.TITLE
        job['date'] = parser.parse(job['date']).timestamp()
        job['finished_processing'] = False
        job['email_sent'] = False

        return job

    @staticmethod
    def _update_array_fields(model, current_values, new_field_values):
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

    def add_jobs(self, jobs, location, search_name):

        new_jobs = {}

        if search_name == IndeedJobSearch.NAME:
            for job in jobs:
                found_job = self.jobs_collection.find_one({'jobkey': job['jobkey']})
                if not found_job:
                    new_jobs[job['jobkey']] = self._setup_new_indeed_job(job, location, search_name)

        if new_jobs:
            return self.jobs_collection.insert_many(new_jobs.values())

    def get_jobs_for_email(self):
        return list(self.jobs_collection.find({'finished_processing': True, 'email_sent': False}))

    def get_newest_job(self, location, search_name):
        self.jobs_collection.find_one(
            {'search_location': location, 'search_engine': search_name},
            sort=[('date', DESCENDING)])

    def process_all_jobs(self):
        db_jobs = self.jobs_collection.find({'finished_processing': False})
        db_jobs_count = db_jobs.count()
        for job_i, job in enumerate(db_jobs):
            self.logger.debug('Processing job {:>3}/{:<3}'.format(job_i + 1, db_jobs_count))

            html_posting = requests.get(job['url']).content
            self.jobs_collection.update_one(
                {'_id': job['_id']},
                {'$set': {
                    'html_posting': html_posting,
                    'finished_processing': True,
                    'email_sent': not self.job_posting_matches(job['jobtitle'], html_posting)}})

    def set_emailed_jobs(self, jobs):
        self.jobs_collection.update_many(
            {'_id:': {'$in': [job['_id'] for job in jobs]}},
            {'$set': {'email_sent': True}})

    @abc.abstractmethod
    def job_posting_matches(self, job_title_posting, html_posting):
        """
        Classify if job matches the job posting
        :param job_title_posting: The title of the job
        :param html_posting: The html of the job posting
        :return: False if the job is not correct for the search.
          True if the job is correct for the search or by default.
        """
        raise NotImplemented()


class MachineLearningJob(AbstractJob):
    TITLE = 'machine learning'

    def __init__(self, database, logger):
        super(MachineLearningJob, self).__init__(database.jobs, logger)

    def job_posting_matches(self, job_title_posting, html_posting):
        """
        Classify if job matches the job posting
        :param job_title_posting: The title of the job
        :param html_posting: The html of the job posting
        :return: False if the job is not correct for the search.
          True if the job is correct for the search or by default.
        """
        regex_keyword_title = re.compile(r'\b(data|machine learning)\b', flags=re.IGNORECASE)
        regex_bad_position_title = re.compile(r'\b(manager|principal|professor|director|lead)\b', flags=re.IGNORECASE)

        job_posting = BeautifulSoup(html_posting, 'html.parser').get_text()
        regex_language_posting = re.compile(r'python', flags=re.IGNORECASE)

        return regex_keyword_title.search(job_title_posting) and \
               not regex_bad_position_title.search(job_title_posting) and \
               regex_language_posting.search(job_posting)
