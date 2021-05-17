import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import _Neo4jDateTimeInput
from politylink.utils import filter_dict_by_value

LOGGER = logging.getLogger(__name__)
DATE_FORMAT = '%Y-%m-%d'
# JSON_FP = '/home/ec2-user/politylink/politylink-tools/wordcloud/minutes/tfidf.json'
JSON_FP = '/Users/musui/politylink/politylink-tools/wordcloud/minutes/tfidf.json'

gql_client = GraphQLClient()


def calc_tfidfs(start_date_str: str, end_date_str: str, interval: int = 0, num_items: int = 200, *,
                committee=None, diet_number=None):
    """
    Windowごとにtfとtfidfを算出し、リストで返す。日付は半開区間で指定する。

    :param start_date_str: '2020-10-26'
    :param end_date_str: '2020-12-06'
    :param interval: tf/tfidfを算出する日付のwindow幅
    :param num_items: tf/tfidfの最大要素数
    :param committee: 委員会
    :param diet_number: 国会回次
    """

    if diet_number:
        diet = number2diet[int(diet_number)]
        start_date = to_datetime_dt(diet.start_date)
        end_date = to_datetime_dt(diet.end_date) + timedelta(days=1)
    else:
        start_date = datetime.strptime(start_date_str, DATE_FORMAT)
        end_date = datetime.strptime(end_date_str, DATE_FORMAT)
    LOGGER.debug(f'calc tfidfs for range({start_date}, {end_date}, {interval}), '
                 f'committee={committee}, diet_number={diet_number}')
    windows = get_all_windows(start_date, end_date, interval)
    LOGGER.debug(f'get {len(windows)} windows')

    response = []
    for start_date, end_date in windows:
        minutes_ids = get_target_minutes_ids(start_date, end_date, committee)
        LOGGER.debug(f'found {len(minutes_ids)} minutes for [{start_date}, {end_date})')
        term_stats = merge_term_stats(minutes_ids)
        tfidfs = {t: s[1] for t, s in term_stats.items()}
        tfidfs = filter_dict_by_value(tfidfs, num_items)
        tfs = {t: term_stats[t][0] for t in tfidfs.keys()}
        response.append({
            "start": start_date.strftime(DATE_FORMAT),
            "end": end_date.strftime(DATE_FORMAT),
            "tf": tfs,
            "tfidf": tfidfs,
        })
    return response


def get_all_windows(start_date: datetime, end_date: datetime, interval: int = 0):
    windows = []
    if interval == 0:
        windows.append((start_date, end_date))
    else:
        window_start_date = start_date
        # ToDo: handle interval != 7
        window_end_date = start_date + timedelta(days=(interval - start_date.weekday()))
        while window_end_date <= end_date:
            windows.append((window_start_date, window_end_date))
            window_start_date = window_end_date
            window_end_date = window_start_date + timedelta(days=interval)
        if window_start_date < end_date:
            windows.append((window_start_date, end_date))
    return windows


def get_target_minutes_ids(start_date, end_date, committee=None):
    minutes_ids = []
    for minutes in all_minutes:  # ToDo: use date->minutes map instead of full scan
        minutes_date = to_datetime_dt(minutes.start_date_time)
        if minutes.ndl_min_id and start_date <= minutes_date < end_date and \
                (committee is None or committee in minutes.name):
            minutes_ids.append(minutes.id)
    return minutes_ids


def merge_term_stats(minutes_ids):
    merged = defaultdict(lambda: [0, 0])  # (tf, tfidf)
    for id_ in minutes_ids:
        if id_ in minutes2ts:
            term_stats = minutes2ts[id_]
            for term, stats in term_stats.items():
                merged[term][0] += stats[0]
                merged[term][1] += stats[1]
    return merged


def to_neo4j_dt(dt):
    return _Neo4jDateTimeInput(year=dt.year, month=dt.month, day=dt.day)


def to_datetime_dt(dt):
    return datetime(year=dt.year, month=dt.month, day=dt.day)


def load_minutes_to_term_stats(json_fp):
    global minutes2ts
    try:
        with open(json_fp, 'r') as f:
            minutes2ts = json.load(f)
    except Exception:
        LOGGER.exception(f'failed to load minutes term stats from {json_fp}')
        return False
    LOGGER.info(f'loaded minutes term stats from {json_fp}')
    return True


all_minutes = gql_client.get_all_minutes(fields=["id", "ndl_min_id", "name", "start_date_time"])
all_diets = gql_client.get_all_diets(fields=["number", "start_date", "end_date"])
number2diet = dict([(diet.number, diet) for diet in all_diets])
minutes2ts = None
load_minutes_to_term_stats(JSON_FP)

# for debug
if __name__ == '__main__':
    print(calc_tfidfs('2020-10-26', '2020-12-06', 7, num_items=5))
