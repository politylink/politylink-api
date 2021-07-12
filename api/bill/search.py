import logging

import stringcase
import time
from elasticsearch_dsl import Search, AttrList

from politylink.elasticsearch.client import ElasticsearchClient
from politylink.elasticsearch.schema import BillText, BillStatus
from politylink.graphql.client import GraphQLClient
from politylink.utils.bill import extract_bill_number_or_none

LOGGER = logging.getLogger(__name__)
es_client = ElasticsearchClient()
gql_client = GraphQLClient(url='https://graphql.politylink.jp')

GQL_FIELDS = ['id', 'name', 'bill_number', 'category', 'tags', 'total_news', 'total_minutes', 'urls']


def search_bills(query: str, categories=None, statuses=None, belonged_to_diets=None, submitted_diets=None,
                 submitted_groups=None, supported_groups=None, opposed_groups=None,
                 full_text=False, page: int = 1, num_items: int = 3, fragment_size: int = 100):
    s = Search(using=es_client.client, index=BillText.index) \
        .source(excludes=[BillText.Field.BODY, BillText.Field.SUPPLEMENT])
    if query:
        maybe_bill_number = extract_bill_number_or_none(query)
        if maybe_bill_number:
            s = s.query('match_phrase', bill_number=maybe_bill_number)
        else:
            fields = [BillText.Field.TITLE + "^100", BillText.Field.TAGS + "^100", BillText.Field.ALIASES + "^100",
                      BillText.Field.BILL_NUMBER + "^100", BillText.Field.REASON + "^10"]
            if full_text:
                fields += [BillText.Field.BODY, BillText.Field.SUPPLEMENT]
            s = s.query('function_score',
                        query={'multi_match': {'query': query, 'fields': fields}},
                        functions=[{'gauss': {BillText.Field.LAST_UPDATED_DATE: {'scale': '180d', 'decay': 0.8}}}]) \
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
    if submitted_groups:
        s = s.filter('terms', submitted_groups=submitted_groups)
    if supported_groups:
        s = s.filter('terms', supported_groups=supported_groups)
    if opposed_groups:
        s = s.filter('terms', opposed_groups=opposed_groups)

    idx_from = (page - 1) * num_items
    idx_to = page * num_items
    s = s[idx_from: idx_to]
    LOGGER.debug(s.to_dict())

    start_time_ms = time.time() * 1000
    es_response = s.execute()
    end_time_ms = time.time() * 1000
    LOGGER.debug(f'took {end_time_ms - start_time_ms} for elasticsearch')

    start_time_ms = time.time() * 1000
    bill_info_map = fetch_gql_bill_info_map([hit.id for hit in es_response.hits])
    end_time_ms = time.time() * 1000
    LOGGER.debug(f'took {end_time_ms - start_time_ms} for GraphQL')

    return build_response(es_response, bill_info_map, fragment_size)


def build_response(es_response, bill_info_map, fragment_size):
    bill_records = []
    for hit in es_response.hits:
        bill_id = hit.id
        if bill_id in bill_info_map:
            bill_info = bill_info_map.get(bill_id)
            bill_records.append(build_bill_record(hit, bill_info, fragment_size))
        else:
            LOGGER.warning(f'failed to fetch {bill_id} from GraphQL')
    return {
        'totalBills': es_response.hits.total.value,
        'bills': bill_records
    }


def build_bill_record(hit, bill_info, fragment_size):
    record = {'id': hit.id}
    record.update(bill_info)

    fragment = None
    if hasattr(hit.meta, 'highlight'):
        for field in [BillText.Field.REASON, BillText.Field.BODY, BillText.Field.SUPPLEMENT]:
            if field.value in hit.meta.highlight:
                fragment = hit.meta.highlight[field.value][0]
                break
    if not fragment:
        fragment = hit.reason[:fragment_size]
    if fragment[-1] != '。':
        fragment += '...'
    record['fragment'] = fragment

    status_index = hit.status if hasattr(hit, 'status') else 0
    record['statusLabel'] = BillStatus.from_index(status_index).label

    es_fields = [BillText.Field.SUBMITTED_DATE, BillText.Field.LAST_UPDATED_DATE,
                 BillText.Field.SUBMITTED_DIET, BillText.Field.BELONGED_TO_DIETS]
    for es_field in es_fields:
        es_field = es_field.value
        if hasattr(hit, es_field):
            value = getattr(hit, es_field)
            record[stringcase.camelcase(es_field)] = list(value) if isinstance(value, AttrList) else value

    return record


def fetch_gql_bill_info_map(bill_ids):
    bill_info_map = dict()
    if not bill_ids:
        return bill_info_map
    bills = gql_client.bulk_get(bill_ids, fields=GQL_FIELDS)
    for bill in bills:
        bill_info_map[bill.id] = {
            'name': bill.name,
            'billNumber': bill.bill_number,
            'billNumberShort': extract_bill_number_or_none(bill.bill_number, short=True),
            'tags': bill.tags if bill.tags else list(),
            'totalNews': bill.total_news,
            'totalMinutes': bill.total_minutes,
            'totalPdfs': sum('PDF' in url.title for url in bill.urls)
        }
    return bill_info_map
