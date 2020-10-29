import asyncio
import pytest
from unittest import mock

from pmgaiorest.testing.fixtures import session, base_session, resp200

from pmgmailchimpapi import api

@pytest.mark.usefixtures('session')
@pytest.fixture
def list_api(session):
    return api.MailchimpApi(session, 'base', 'key', 'user', list_id='lid')

MEMBERS = [
        api.Subscriber('fname1', 'lname1', 'email1'),
        api.Subscriber('fname2', 'lname2', 'email2'),
        api.Subscriber('fname3', 'lname3', 'email3'),
        api.Subscriber('fname4', 'lname4', 'email4'),
        api.Subscriber('fname5', 'lname5', 'email5'),
        ]

EXPECTED_CALLS = [
        mock.call('base/lists/lid',
            headers=mock.ANY,
            json={'members':
                [
                    {'email_address': 'email1',
                        'merge_fields': {'FNAME': 'fname1', 'LNAME': 'lname1'},
                        'status': 'subscribed'},
                    {'email_address': 'email2',
                        'merge_fields': {'FNAME': 'fname2', 'LNAME': 'lname2'},
                        'status': 'subscribed'},
                    ],
                'update_existing': True},
            auth=mock.ANY),
        mock.call('base/lists/lid',
            headers=mock.ANY,
            json={'members':
                [
                    {'email_address': 'email3',
                        'merge_fields': {'FNAME': 'fname3', 'LNAME': 'lname3'},
                        'status': 'subscribed'},
                    {'email_address': 'email4',
                        'merge_fields': {'FNAME': 'fname4', 'LNAME': 'lname4'},
                        'status': 'subscribed'},
                    ],
                'update_existing': True},
            auth=mock.ANY),
        mock.call('base/lists/lid',
            headers=mock.ANY,
            json={'members':
                [
                    {'email_address': 'email5',
                        'merge_fields': {'FNAME': 'fname5', 'LNAME': 'lname5'},
                        'status': 'subscribed'}
                    ],
                'update_existing': True},
            auth=mock.ANY),
        ]

@pytest.mark.usefixtures('session', 'list_api')
@pytest.mark.asyncio
async def test_bach_subscribe_sync(session, list_api):
    api.BATCH_LIMIT = 2
    await list_api.batch_subscribe(MEMBERS)
    session.post.assert_has_calls(EXPECTED_CALLS)

@pytest.mark.usefixtures('session', 'list_api')
@pytest.mark.asyncio
async def test_bach_subscribe_async(session, list_api):
    api.BATCH_LIMIT = 2
    async def members():
        for m in MEMBERS:
            yield m
    await list_api.batch_subscribe(members())
    session.post.assert_has_calls(EXPECTED_CALLS)


    #session.get.assert_called_with(REQ_URL, auth=auth, headers=REQ_BASE_HEADERS)
