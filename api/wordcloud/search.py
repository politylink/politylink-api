from elasticsearch_dsl import Search
from politylink.elasticsearch.client import ElasticsearchClient
from politylink.elasticsearch.schema import SpeechText
from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import _SpeechFilter, Query
from sgqlc.operation import Operation

es_client = ElasticsearchClient()
gql_client = GraphQLClient()


def search_speech(term: str, start_date_str: str, end_date_str: str, committee: str = None,
                  num_items: int = 3, fragment_size: int = 100):
    s = Search(using=es_client.client, index=SpeechText.index) \
        .filter('range', **{SpeechText.Field.DATE: {'gte': start_date_str, 'lt': end_date_str}}) \
        .query('multi_match', query=term, fields=[SpeechText.Field.BODY]) \
        .source(excludes=[SpeechText.Field.BODY]) \
        .highlight(SpeechText.Field.BODY, fragment_size=fragment_size, number_of_fragments=1,
                   pre_tags=['<b>'], post_tags=['</b>'])
    if committee:
        s = s.query('match', title=committee)
    s = s[:num_items]
    response = s.execute()

    records = []
    for hit in response.hits:
        record = {
            'speech_id': hit.id,
            'speaker': hit.speaker,
            'date': hit.date,
            'body': hit.meta.highlight.body[0]
        }
        record.update(fetch_gql_speech(record['speech_id']))
        records.append(record)
    return records


# TODO: support batch request
def fetch_gql_speech(speech_id):
    op = Operation(Query)
    speech = op.speech(filter=_SpeechFilter({'id': speech_id}))
    speech.id()
    speech.order_in_minutes()
    minutes = speech.belonged_to_minutes()
    minutes.id()
    minutes.name()
    minutes.ndl_min_id()
    member = speech.be_delivered_by_member()
    member.id()
    member.name()

    res = gql_client.endpoint(op)
    speech = (op + res).speech[0]
    minutes = speech.belonged_to_minutes
    member = speech.be_delivered_by_member

    speech_info = {'speech_id': speech.id}
    if minutes:
        speech_info['minutes_id'] = minutes.id
        speech_info['minutes_name'] = minutes.name
        speech_info['minutes_politylink_url'] = to_politylink_url(minutes.id)
        if minutes.ndl_min_id:
            speech_info['speech_ndl_url'] = 'https://kokkai.ndl.go.jp/txt/{0}/{1}'.format(
                minutes.ndl_min_id, speech.order_in_minutes
            )
    if member:
        speech_info['member_id'] = member.id
        speech_info['member_name'] = member.name
        speech_info['member_image_url'] = to_politylink_url(member.id, domain='image.politylink.jp')
        speech_info['member_politylink_url'] = to_politylink_url(member.id)
    return speech_info


def to_politylink_url(politylink_id, domain='politylink.jp'):
    class_, base = politylink_id.split(':')
    return 'https://{0}/{1}/{2}'.format(domain, class_.lower(), base)
