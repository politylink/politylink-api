import logging
import re

from elasticsearch_dsl import Search
from politylink.elasticsearch.client import ElasticsearchClient
from politylink.elasticsearch.schema import BillText
from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)
es_client = ElasticsearchClient()
gql_client = GraphQLClient(url='https://graphql.politylink.jp')


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
    response = s.execute()

    records = []
    for hit in response.hits:
        bill_id = hit.id
        record = {'id': bill_id}

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

        record.update(fetch_gql_bill_info(bill_id))

        records.append(record)
    return records


def fetch_gql_bill_info(bill_id):
    bill = gql_client.get(bill_id)
    bill_info = {
        'name': bill.name,
        'bill_number': bill.bill_number,
        'bill_number_short': to_bill_number_short(bill.bill_number),
        'status_label': get_status_label(bill),
        'tags': bill.tags if bill.tags else list(),
        'total_news': bill.total_news,
        'total_minutes': bill.total_minutes
    }
    return bill_info


def to_bill_number_short(bill_number):
    pattern = '第([0-9]+)回国会([衆|参|閣])法第([0-9]+)号'
    m = re.match(pattern, bill_number)
    return '-'.join(m.groups())


def get_status_label(bill):
    fields = ['submitted_date', 'passed_representatives_committee_date', 'passed_representatives_date',
              'passed_councilors_committee_date', 'passed_councilors_date', 'proclaimed_date']
    labels = ['提出', '衆委可決', '衆可決', '参委可決', '参可決', '公布']

    max_date = ''
    max_label = '提出'
    for field, label in zip(fields, labels):
        if hasattr(bill, field):
            field_date = getattr(bill, field).formatted
            if field_date and field_date >= max_date:  # >= to prioritize later label
                max_date = field_date
                max_label = label
    return max_label
