import logging
import re

import stringcase
import time
from elasticsearch_dsl import Search, AttrList

from politylink.elasticsearch.client import ElasticsearchClient
from politylink.elasticsearch.schema import BillText, BillStatus
from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)
es_client = ElasticsearchClient()
gql_client = GraphQLClient(url='https://graphql.politylink.jp')

GQL_FIELDS = ['id', 'name', 'bill_number', 'category', 'tags', 'total_news', 'total_minutes', 'urls']


def search_bills(query: str, categories=None, statuses=None, belonged_to_diets=None, submitted_diets=None,
                 page: int = 1, num_items: int = 3, fragment_size: int = 100):
    s = Search(using=es_client.client, index=BillText.index) \
        .source(excludes=[BillText.Field.BODY, BillText.Field.SUPPLEMENT])
    if query:
        fields = [BillText.Field.TITLE + "^100", BillText.Field.TAGS + "^100", BillText.Field.ALIASES + "^100",
                  BillText.Field.REASON + "^10",
                  BillText.Field.BODY, BillText.Field.SUPPLEMENT]
        s = s.query('function_score',
                    query={'multi_match': {'query': query, 'fields': fields}},
                    functions=[{'gauss': {BillText.Field.LAST_UPDATED_DATE: {'scale': '30d'}}}]) \
            .highlight(BillText.Field.REASON, BillText.Field.BODY, BillText.Field.SUPPLEMENT,
                       boundary_chars='.,!? \t\n、。',
                       fragment_size=fragment_size, number_of_fragments=1,
                       pre_tags=['<b>'], post_tags=['</b>'])
    else:
        s = s.sort('-' + BillText.Field.LAST_UPDATED_DATE)

    if categories:
        s = s.filter('terms', category=categories)
    if statuses:
        s = s.filter('terms', status=statuses)
    if belonged_to_diets:
        s = s.filter('terms', belonged_to_diets=belonged_to_diets)
    if submitted_diets:
        s = s.filter('terms', submitted_diet=submitted_diets)

    idx_from = (page - 1) * num_items
    idx_to = page * num_items
    s = s[idx_from: idx_to]
    LOGGER.debug(s.to_dict())

    start_time_ms = time.time() * 1000
    es_response = s.execute()
    end_time_ms = time.time() * 1000
    LOGGER.debug(f'took {end_time_ms - start_time_ms} for elasticsearch')

    bill_ids = [hit.id for hit in es_response.hits]

    start_time_ms = time.time() * 1000
    bill_info_map = fetch_gql_bill_info_map(bill_ids)
    end_time_ms = time.time() * 1000
    LOGGER.debug(f'took {end_time_ms - start_time_ms} for GraphQL')

    bill_records = []
    for hit in es_response.hits:
        bill_id = hit.id
        if bill_id not in bill_info_map:
            LOGGER.warning(f'failed to fetch {bill_id} from GraphQL')
            continue

        record = {'id': bill_id}
        record.update(bill_info_map[bill_id])

        if hasattr(hit.meta, 'highlight'):
            for field in [BillText.Field.REASON, BillText.Field.BODY, BillText.Field.SUPPLEMENT]:
                if field.value in hit.meta.highlight:
                    record['fragment'] = hit.meta.highlight[field.value][0]
                    break
        if 'fragment' not in record:
            record['fragment'] = hit.reason[:fragment_size]
        if record['fragment'][-1] != '。':
            record['fragment'] += '...'

        record['statusLabel'] = BillStatus.from_index(hit.status)

        es_fields = [BillText.Field.SUBMITTED_DATE, BillText.Field.LAST_UPDATED_DATE,
                     BillText.Field.SUBMITTED_DIET, BillText.Field.BELONGED_TO_DIETS]
        for es_field in es_fields:
            es_field = es_field.value
            if hasattr(hit, es_field):
                value = getattr(hit, es_field)
                record[stringcase.camelcase(es_field)] = list(value) if isinstance(value, AttrList) else value

        bill_records.append(record)
    return {
        'totalBills': es_response.hits.total.value,
        'bills': bill_records
    }


def fetch_gql_bill_info_map(bill_ids):
    bill_info_map = dict()
    if not bill_ids:
        return bill_info_map
    bills = gql_client.bulk_get(bill_ids, fields=GQL_FIELDS)
    for bill in bills:
        bill_info_map[bill.id] = {
            'name': bill.name,
            'billNumber': bill.bill_number,
            'billNumberShort': to_bill_number_short(bill.bill_number),
            'tags': bill.tags if bill.tags else list(),
            'totalNews': bill.total_news,
            'totalMinutes': bill.total_minutes,
            'totalPdfs': sum('PDF' in url.title for url in bill.urls)
        }
    return bill_info_map


def to_bill_number_short(bill_number):
    pattern = '第([0-9]+)回国会([衆|参|閣])法第([0-9]+)号'
    m = re.match(pattern, bill_number)
    return '-'.join(m.groups())
