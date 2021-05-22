import logging
import re

from elasticsearch_dsl import Search
from politylink.elasticsearch.client import ElasticsearchClient
from politylink.elasticsearch.schema import BillText
from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)
es_client = ElasticsearchClient()
gql_client = GraphQLClient(url='https://graphql.politylink.jp')

GQL_FIELDS = ['id', 'name', 'bill_number', 'category', 'tags', 'total_news', 'total_minutes']
GQL_DATE_FIELDS = ['submitted_date', 'passed_representatives_committee_date', 'passed_representatives_date',
                   'passed_councilors_committee_date', 'passed_councilors_date', 'proclaimed_date']
GQL_DATE_LABELS = ['提出', '衆委可決', '衆可決', '参委可決', '参可決', '公布']


def search_bills(query: str, num_items: int = 3, fragment_size: int = 100):
    s = Search(using=es_client.client, index=BillText.index) \
        .source(excludes=[BillText.Field.BODY, BillText.Field.SUPPLEMENT]) \
        .sort('-' + BillText.Field.LAST_UPDATED_DATE)
    if query:
        s = s.query('multi_match', query=query,
                    fields=[BillText.Field.TITLE, BillText.Field.REASON, BillText.Field.BODY,
                            BillText.Field.TAGS, BillText.Field.ALIASES]) \
            .highlight(BillText.Field.REASON, BillText.Field.BODY,
                       boundary_chars='.,!? \t\n、。',
                       fragment_size=fragment_size, number_of_fragments=1,
                       pre_tags=['<b>'], post_tags=['</b>'])
    s = s[:num_items]
    es_response = s.execute()

    if not es_response.hits:
        return list()

    bill_ids = [hit.id for hit in es_response.hits]
    bill_info_map = fetch_gql_bill_info_map(bill_ids)
    bill_records = []
    for hit in es_response.hits:
        bill_id = hit.id
        if bill_id not in bill_info_map:
            LOGGER.warning(f'failed to fetch {bill_id} from GraphQL')
            continue

        record = {'id': bill_id}
        record.update(bill_info_map[bill_id])

        if hasattr(hit.meta, 'highlight') and 'reason' in hit.meta.highlight:
            record['fragment'] = hit.meta.highlight['reason'][0]
        elif hasattr(hit.meta, 'highlight') and 'body' in hit.meta.highlight:
            record['fragment'] = hit.meta.highlight['body'][0]
        else:
            record['fragment'] = hit.reason[:fragment_size]
        if record['fragment'][-1] != '。':
            record['fragment'] += '...'

        es_fields = ['submitted_date', 'last_updated_date']
        for es_field in es_fields:
            if hasattr(hit, es_field):
                record[es_field] = getattr(hit, es_field)

        bill_records.append(record)
    return bill_records


def fetch_gql_bill_info_map(bill_ids):
    bills = gql_client.bulk_get(bill_ids, fields=GQL_FIELDS + GQL_DATE_FIELDS)
    bill_info_map = dict()
    for bill in bills:
        bill_info_map[bill.id] = {
            'name': bill.name,
            'bill_number': bill.bill_number,
            'bill_number_short': to_bill_number_short(bill.bill_number),
            'status_label': get_status_label(bill),
            'tags': bill.tags if bill.tags else list(),
            'total_news': bill.total_news,
            'total_minutes': bill.total_minutes
        }
    return bill_info_map


def to_bill_number_short(bill_number):
    pattern = '第([0-9]+)回国会([衆|参|閣])法第([0-9]+)号'
    m = re.match(pattern, bill_number)
    return '-'.join(m.groups())


def get_status_label(bill):
    max_date = ''
    max_label = ''
    for field, label in zip(GQL_DATE_FIELDS, GQL_DATE_LABELS):
        if hasattr(bill, field):
            field_date = getattr(bill, field).formatted
            if field_date and field_date >= max_date:  # >= to prioritize later label
                max_date = field_date
                max_label = label
    return max_label
