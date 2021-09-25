from aiohttp import BasicAuth
from aiohttp.client_exceptions import ClientResponseError
from itertools import islice
from pmgaiorest import ApiBase
from logging import getLogger
from collections import namedtuple
from typing import Dict
import hashlib

logger = getLogger(__name__)

Subscriber = namedtuple('Subscriber', 'first_name last_name email_address interests')
BATCH_LIMIT = 500

class MailchimpApi(ApiBase):
    def __init__(self, aiohttp_session, base_url, api_key, user):
        super().__init__(aiohttp_session, base_url, auth=BasicAuth(user, api_key))

    async def get_lists(self):
        return await self.get('lists')

    async def get_campaigns(self, params=None):
        return await self.get('campaigns',params=params or {})

    async def get_campaign_info(self, camp_id):
        return await self.get(f'campaigns/{camp_id}')

    async def get_campaign_content(self, camp_id):
        return await self.get(f'campaigns/{camp_id}/content')

    async def replicate_campaign(self, camp_id):
        return await self.post(f'campaigns/{camp_id}/actions/replicate')

    async def create_campaign(self, json):
        return await self.post(f'campaigns', json=json)

    async def set_campaign_content(self, *,
            camp_id: str,
            template_id: int,
            sections: Dict[str, str]):
        json = {
                'template': {
                    'id': template_id,
                    'sections': sections,
                    }
                }
        return await self.put(f'campaigns/{camp_id}/content', json=json)

    async def set_campaign_settings(self, *, 
            camp_id: str,
            settings: Dict[str, str]):
        json = {'settings': settings}
        return await self.patch(f'campaigns/{camp_id}', json=json)

    async def send_campaign(self, camp_id):
        return await self.post(f'campaigns/{camp_id}/actions/send')

    async def get_templates(self, params=None):
        return await self.get('templates', params=params or {})

    async def get_user_templates(self, params=None):
        params=params or {}
        params['type'] = 'user'
        return await self.get('templates', params=params)

    async def get_template_info(self, temp_id):
        return await self.get(f'templates/{temp_id}')

    async def get_template_default_content(self, temp_id):
        return await self.get(f'templates/{temp_id}/default-content')

    async def create_template(self, *, name, html_file):
        with open(html_file, 'r') as f:
            return await self.post(f'templates',
                    json={'name': name, 'html':f.read()})

    async def update_template(self, *, id, name, html_file):
        with open(html_file, 'r') as f:
            return await self.patch(f'templates/{id}',
                    json={'name': name, 'html':f.read()})

    async def get_list_interest_categories(self, list_id):
        return await self._list_op(self.get, 'interest-categories', list_id=list_id)

    async def get_list_interests(self, *, int_cat_id, list_id):
        return await self._list_op(self.get, f'interest-categories/{int_cat_id}/interests', list_id=list_id)

    async def get_list_member(self, *, list_id, member_email):
        email_hash = _email_to_hash(member_email)
        try:
            return await self._list_op(self.get, f'members/{email_hash}', list_id=list_id)
        except ClientResponseError as e:
            logger.error('Could not find member %s', member_email)
            return None

    async def update_interest_subscriptions(self, *, list_id, member_email, interests):
        email_hash = _email_to_hash(member_email)
        try:
            return await self._list_op(self.patch,
                    f'members/{email_hash}',
                    list_id=list_id,
                    json={'interests': interests}
                    )
        except ClientResponseError as e:
            logger.error('Could not find member %s', member_email)
            return None

    async def get_list_members(self, *, list_id, count=100, offset=0):
        return await self._list_op(self.get, 'members', list_id=list_id,
                params={'count': count, 'offset': offset})

    async def itr_list_members(self, *, list_id):
        offset = 0
        count = 50
        while True:
            page = await self._list_op(self.get, 'members', list_id=list_id,
                    params={'count': count, 'offset': offset})
            for mem in page['members']:
                yield mem
            offset += count
            if offset > page['total_items']:
                break

    async def batch_subscribe(self, members_itr, *, list_id=None):
        if not hasattr(members_itr, '__anext__'):
            async def members():
                for m in members_itr:
                    yield m
            _members_itr = members()
        else:
            _members_itr = members_itr

        while True:
            members = []

            count = 0
            async for member in _members_itr:
                if not member.interests:
                    continue
                mem = {'email_address': member.email_address,
                        'merge_fields': {'FNAME': member.first_name,
                            'LNAME': member.last_name},
                        'status':'subscribed',
                        } 
                if member.interests is not None:
                    mem['interests'] = {int_id: True for int_id in member.interests}

                members.append(mem)
                count += 1
                if count >= BATCH_LIMIT:
                    break

            if not len(members):
                return

            await self._list_op(self.post, 
                    json = {'members': members, 'update_existing': True},
                    list_id=list_id)

    async def get_segments(self, *, list_id, params=None):
        return await self._list_op(self.get, 'segments',
                params=params or {}, list_id=list_id)

    async def archive_member(self, *, list_id, member_id):
        try:
            await self._list_op(self.delete, '/'.join(['members', member_id]), list_id=list_id)
            return True
        except ClientResponseError as e:
            logger.error('Could not archive member %s', member_id)
            return False

    async def _list_op(self, op, path=None, *, list_id, **kwargs):
        list_path = '/'.join(('lists', list_id),)
        if path:
            list_path = '/'.join((list_path, path),)
        return await op(list_path, **kwargs)

def _email_to_hash(email):
    return hashlib.md5(email.encode()).hexdigest()

