import logging

import time
from elasticsearch_dsl import Search
from sgqlc.operation import Operation

from politylink.elasticsearch.client import ElasticsearchClient
from politylink.elasticsearch.schema import MemberText, ParliamentaryGroup, House
from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Query, _MemberFilter, Activity
from politylink.utils import to_date_str

LOGGER = logging.getLogger(__name__)
es_client = ElasticsearchClient()
gql_client = GraphQLClient(url='https://graphql.politylink.jp')

GQL_FIELDS = ['id', 'name', 'name_hira', 'group']


def search_members(query: str, groups=None, houses=None, page: int = 1, num_items: int = 3, fragment_size: int = 100):
    s = Search(using=es_client.client, index=MemberText.index)
    if query:
        fields = [MemberText.Field.NAME + '^100', MemberText.Field.NAME_HIRA + '^100',
                  MemberText.Field.DESCRIPTION + "^10"]
        s = s.query('multi_match', query=query, fields=fields) \
            .highlight(MemberText.Field.DESCRIPTION,
                       boundary_chars='.,!? \t\n、。',
                       fragment_size=fragment_size, number_of_fragments=1,
                       pre_tags=['<b>'], post_tags=['</b>'])
    else:
        s = s.sort('-' + MemberText.Field.LAST_UPDATED_DATE)

    if groups:
        s = s.filter('terms', group=groups)
    if houses:
        s = s.filter('terms', house=houses)

    idx_from = (page - 1) * num_items
    idx_to = page * num_items
    s = s[idx_from: idx_to]
    LOGGER.debug(s.to_dict())

    start_time_ms = time.time() * 1000
    es_response = s.execute()
    end_time_ms = time.time() * 1000
    LOGGER.debug(f'took {end_time_ms - start_time_ms} for elasticsearch')

    start_time_ms = time.time() * 1000
    member_info_map = fetch_gql_member_info_map([hit.id for hit in es_response.hits])
    end_time_ms = time.time() * 1000
    LOGGER.debug(f'took {end_time_ms - start_time_ms} for GraphQL')

    return build_response(es_response, member_info_map, fragment_size)


def build_response(es_response, member_info_map, fragment_size):
    member_records = []
    for hit in es_response.hits:
        member_id = hit.id
        if member_id in member_info_map:
            member_info = member_info_map.get(member_id)
            member_records.append(build_member_record(hit, member_info, fragment_size))
        else:
            LOGGER.warning(f'failed to fetch {member_id} from GraphQL')
    return {
        'totalMembers': es_response.hits.total.value,
        'members': member_records
    }


def build_member_record(hit, member_info, fragment_size):
    record = {'id': hit.id}
    record.update(member_info)

    fragment = None
    if hasattr(hit.meta, 'highlight'):
        for field in [MemberText.Field.DESCRIPTION]:
            if field.value in hit.meta.highlight:
                fragment = hit.meta.highlight[field.value][0]
                break
    if not fragment:
        fragment = hit.description[:fragment_size]
    if fragment[-1] != '。':
        fragment += '...'
    record['fragment'] = fragment

    if hasattr(hit, 'house'):
        record['house'] = House.from_index(hit.house).label

    return record


def fetch_gql_member_info_map(member_ids):
    member_info_map = dict()
    if not member_ids:
        return member_info_map

    op = Operation(Query)
    members = op.member(filter=_MemberFilter({'id_in': member_ids}))

    for field in GQL_FIELDS:
        getattr(members, field)()

    activities = members.activities()  # TODO: fetch only the latest activity once POL-344 is fixed
    activities.datetime()
    bill = activities.bill()
    bill.name()
    minutes = activities.minutes()
    minutes.name()

    res = gql_client.endpoint(op)
    members = (op + res).member

    for member in members:
        member_info = {
            'name': member.name,
            'nameHira': member.name_hira,
        }
        if member.group:
            member_info['group'] = ParliamentaryGroup.from_gql(member.group).label
        if member.activities:
            latest_activity = max(member.activities, key=lambda x: x.datetime.formatted)
            member_info['activity'] = build_activity_info(latest_activity)
        member_info_map[member.id] = member_info
    return member_info_map


def build_activity_info(activity: Activity):
    activity_info = {
        'date': to_date_str(activity.datetime)
    }
    if activity.bill:
        activity_info['type'] = 'bill'
        activity_info['message'] = '{}を提出しました'.format(activity.bill.name)
    elif activity.minutes:
        activity_info['type'] = 'minutes'
        activity_info['message'] = '{}で発言しました'.format(activity.minutes.name)
    else:
        raise ValueError(f'unknown activity type: {activity}')
    return activity_info
