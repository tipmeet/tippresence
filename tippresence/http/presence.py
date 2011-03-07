# -*- coding: utf-8 -*-

import json

from twisted.internet import defer
from twisted.web import resource, server, http

from tippresence import stats
from tippresence import aggregate_status
from tippresence import PresenceServiceError

from twisted.python import log

success_reply = {'status': 'ok', 'reason': 'Success'}

class HTTPPresence(resource.Resource):
    isLeaf = True
    def __init__(self, presence, users=None):
        self.presence = presence
        self.users = users or {}

    def _filterPath(self, path):
        return [x for x in path if x]

    def render_GET(self, request):
        stats['http_received_requests'] += 1
        path = self._filterPath(request.postpath)
        if len(path) == 1:
            return self.getStatus(request.write, request.finish, path[0])
        elif len(path) == 0:
            if not self.authenticate(request):
                return json.dumps({'status': 'failure', 'reason': 'auth required'})
            return self.dumpStatuses(request.write, request.finish)
        return json.dumps({'reason': 'Invalid URI', 'status': 'failure'})

    def render_PUT(self, request):
        stats['http_received_requests'] += 1
        if not self.authenticate(request):
            return json.dumps({'status': 'failure', 'reason': 'auth required'})
        path = self._filterPath(request.postpath)
        if len(path) == 1:
            return self.putStatus(request.write, request.finish, path[0], request.content)
        if len(path) == 2:
            return self.putStatus(request.write, request.finish, path[0], request.content, tag=path[1])
        return json.dumps({'reason': 'Invalid URI', 'status': 'failure'})

    def render_DELETE(self, request):
        stats['http_received_requests'] += 1
        if not self.authenticate(request):
            return json.dumps({'status': 'failure', 'reason': 'auth required'})
        path = self._filterPath(request.postpath)
        if len(path) == 2:
            return self.removeStatus(request.write, request.finish, path[0], path[1])
        return json.dumps({'status': 'failure', 'reason': 'Invalid URI'})

    def render_POST(self, request):
        stats['http_received_requests'] += 1
        if not self.authenticate(request):
            return json.dumps({'status': 'failure', 'reason': 'auth required'})
        path = self._filterPath(request.postpath)
        if path:
            return json.dumps({'status': 'failure', 'reason': 'Invalid URI'})
        return self.putAllStatuses(request.write, request.finish, request.content)

    def authenticate(self, request):
        if not self.users:
            return 1
        user, password = request.getUser(), request.getPassword()
        if not user or not password:
            log.msg("AUTH: Request without auth token")
            request.setHeader('WWW-Authenticate', 'Basic realm="tippresence"')
            request.setResponseCode(http.UNAUTHORIZED)
            return
        if user not in self.users:
            log.msg("AUTH: User %s not found" % user)
            return
        if password != self.users[user]:
            log.msg("AUTH: Invalid password for user %s" % user)
            return
        return 1

    def getStatus(self, write, finish, resource):
        def reply(status):
            res = {'status': 'ok', 'reason': 'success'}
            res['result'] = status
            write(json.dumps(res))
            finish()

        d = self.presence.getStatus(resource)
        d.addCallback(aggregate_status)
        d.addCallback(reply)
        return server.NOT_DONE_YET

    def dumpStatuses(self, write, finish):
        def reply(r):
            result = {}
            for res, status in r.items():
                result[res] = {}
                result[res] = aggregate_status(status)
            write(json.dumps({'status': 'ok', 'reason': 'Successfully dumped', 'result': result}))
            finish()

        d = self.presence.dumpStatuses()
        d.addCallback(reply)
        return server.NOT_DONE_YET

    def putStatus(self, write, finish, resource, content, tag=None):
        def reply(tag):
            result = {'tag': tag}
            r = json.dumps({'reason': 'Status added', 'status': 'ok', 'result': result})
            write(r)
            finish()

        def reply_error(failure):
            failure.trap(PresenceServiceError)
            msg = failure.getErrorMessage()
            r = json.dumps({'status': 'failure', 'reason': msg})
            write(r)
            finish()

        try:
            r = json.load(content)
        except ValueError, e:
            return json.dumps({'reason': str(e), 'status': 'failure'})
        args = []
        kw = {}
        args.append(resource)
        args.append(r['presence'])
        if not 'expires' in r:
            return json.dumps({'reason': 'Expires required', 'status': 'failure'})
        args.append(int(r['expires']))
        if 'priority' in r:
            kw['priority'] = int(r['priority'])
        if tag:
            kw['tag'] = tag
        d = self.presence.putStatus(*args, **kw)
        d.addCallback(reply)
        d.addErrback(reply_error)
        return server.NOT_DONE_YET

    def removeStatus(self, write, finish, resource, tag):
        def reply(r):
            r = json.dumps({'reason': r, 'status': 'ok'})
            write(r)
            finish()

        d = self.presence.removeStatus(resource, tag)
        d.addCallback(reply)
        return server.NOT_DONE_YET

    def putAllStatuses(self, write, finish, content):
        def reply(r):
            write(json.dumps({'reason': 'Success', 'status': 'ok'}))
            finish()
        def reply_error(r):
            write(json.dumps({'reason': 'Failed: %s' % str(r), 'status': 'failure'}))
            finish()

        try:
            docs = json.load(content)
        except ValueError, e:
            return json.dumps({'reason': str(e), 'status': 'failure'})
        d = []
        for (resource, r) in docs.items():
            args = []
            kw = {}
            args.append(resource)
            args.append(r['presence'])
            args.append(int(r['expires']))
            if 'priority' in r:
                kw['priority'] = int(r['priority'])
            if 'tag' in r:
                kw['tag'] = r['tag']
            d.append(self.presence.putStatus(*args, **kw))
        defer.DeferredList(d, fireOnOneErrback=True).addCallbacks(reply, reply_error)
        return server.NOT_DONE_YET

