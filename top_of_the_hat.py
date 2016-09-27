from argparse import ArgumentParser
from configparser import ConfigParser
import logging

from indeed import IndeedClient
from pymongo import MongoClient

from scrape import scrape_indeed, scrape_cities


def run():
    app_name = 'top_of_the_pile'

    # Parse config file
    config_parser = ConfigParser()
    config_parser.read('config.ini')

    # Parse arguments
    arg_parser = ArgumentParser()
    arg_parser.add_argument('TaskType', help='The task to run', choices=['monitor_indeed'])
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

    cities = scrape_cities()
    scrape_indeed(database, indeed_api, logger, 'machine learning', cities)


if __name__ == '__main__':
    run()
