from aiohttp import BasicAuth
from itertools import islice
from pmgaiorest import ApiBase
from logging import getLogger
from collections import namedtuple
from typing import Dict

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

    async def get_list_interests(self, int_cat_id):
        return await self._get_list(f'interest-categories/{int_cat_id}/interests')

    async def get_list_interest_categories(self):
        return await self._get_list('interest-categories')

    async def get_list_members(self, *, list_id):
        return await self._list_op(self.get, 'members', list_id=list_id)

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

                members.append({'email_address': member.email_address,
                        'merge_fields': {'FNAME': member.first_name,
                            'LNAME': member.last_name},
                        'status':'subscribed',
                        'interests': {int_id: True for int_id in member.interests}
                        })
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

    async def _list_op(self, op, path=None, *, list_id, **kwargs):
        list_path = '/'.join(('lists', list_id),)
        if path:
            list_path = '/'.join((list_path, path),)
        return await op(list_path, **kwargs)

