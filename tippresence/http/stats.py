# -*- coding: utf-8 -*-

import json

from twisted.internet import defer
from twisted.web import resource

class HTTPStats(resource.Resource):
    isLeaf = True

    def __init__(self, presence_service):
        self.presence_service = presence_service

    def _dump(self):
        r = {}
        r['presence_put'] = self.presence_service.stats_put
        r['presence_gotten'] = self.presence_service.stats_get
        r['presence_removed'] = self.presence_service.stats_remove
        r['presence_updated'] = self.presence_service.stats_update
        r['presence_dumped'] = self.presence_service.stats_dump
        r['active_presence'] = self.presence_service.stats_active_presence
        return r

    def render_GET(self, request):
        return json.dumps(self._dump(), indent=4)

