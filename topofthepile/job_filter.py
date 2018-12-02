import abc
import re
import requests

from bs4 import BeautifulSoup


class AbstractJobFilter(abc.ABC):
    @abc.abstractmethod
    def _job_matches_search(self, search, job_title, html_posting):
        """
        Classify if job matches the search term
        :param search: The job search term
        :param job_title: The title of the job
        :param html_posting: The html of the job posting
        :return: False if the job is not correct for the search.
          True if the job is correct for the search or by default.
        """
        raise NotImplemented()

    def __init__(self, database, logger):
        self.database = database
        self.logger = logger

    def filter_jobs(self, searched_job_title, db_jobs):
        total_jobs = db_jobs.count()
        for job_i, job in enumerate(db_jobs):
            self.logger.debug('Processing job {:>3}/{:<3}'.format(job_i + 1, total_jobs))
            html_posting = requests.get(job['url']).content
            self.database.jobs.update_one(
                {'_id': job['_id']},
                {'$set': {
                    'html_posting': html_posting,
                    'finished_processing': True,
                    'email_sent': not self._job_matches_search(searched_job_title, job['jobtitle'], html_posting)}})


class MachineLearningJobFilter(AbstractJobFilter):
    def _job_matches_search(self, search, job_title, html_posting):
        """
        Classify if job matches the search term
        :param search: The job search term
        :param job_title: The title of the job
        :param html_posting: The html of the job posting
        :return: False if the job is not correct for the search.
          True if the job is correct for the search or by default.
        """
        regex_keyword_title = re.compile(r'\b(data|machine learning)\b', flags=re.IGNORECASE)
        regex_bad_position_title = re.compile(r'\b(manager|principal|professor|director|lead)\b', flags=re.IGNORECASE)

        job_posting = BeautifulSoup(html_posting, 'html.parser').get_text()
        regex_language_posting = re.compile(r'python', flags=re.IGNORECASE)

        return regex_keyword_title.search(job_title) and \
            not regex_bad_position_title.search(job_title) and \
            regex_language_posting.search(job_posting)
