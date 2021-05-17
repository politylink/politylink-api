from politylink.graphql.schema import Bill, _Neo4jDateTime

from api.bill.search import get_status_label, to_bill_number_short


def test_to_bill_number_short():
    assert '204-衆-6' == to_bill_number_short('第204回国会衆法第6号')


def test_get_status_label():
    bill = Bill(None)
    bill.submitted_date = _Neo4jDateTime({'formatted': '2021-01-01'})
    bill.passed_representatives_committee_date = _Neo4jDateTime({'formatted': '2021-01-02'})
    bill.passed_representatives_date = _Neo4jDateTime({'formatted': '2021-01-02'})
    assert '衆可決' == get_status_label(bill)
