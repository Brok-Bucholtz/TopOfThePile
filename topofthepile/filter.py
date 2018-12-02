import re

from bs4 import BeautifulSoup


def _custom_filter(job_title, html_posting):
    regex_keyword_title = re.compile(r'\b(data|machine learning)\b', flags=re.IGNORECASE)
    regex_bad_position_title = re.compile(r'\b(manager|principal|professor|director|lead)\b', flags=re.IGNORECASE)

    job_posting = BeautifulSoup(html_posting, 'html.parser').get_text()
    regex_language_posting = re.compile(r'python', flags=re.IGNORECASE)

    return regex_keyword_title.search(job_title) and\
        not regex_bad_position_title.search(job_title) and\
        regex_language_posting.search(job_posting)


def job_matches_search(search, job_title, html_posting):
    """
    Classify if job matches the search term
    :param search: The job search term
    :param job_title: The title of the job
    :param html_posting: The html of the job posting
    :return: False if the job is not correct for the search.  True if the job is correct for the search or by default.
    """
    # Add or remove filters that best work for your personal search criteria
    if search in ['machine learning', 'data analytics', 'data analysis']:
        return _custom_filter(job_title, html_posting)

    return True
