# -*- coding: utf-8 -*-

import json

from twisted.internet import defer
from twisted.web import resource, server, http

from tippresence import PresenceError

from twisted.python import log

success_reply = {'status': 'ok', 'reason': 'Success'}

def debug(msg):
    if __debug__:
        log.msg(msg)

def response(status='failure', reason='Failed', result=None):
    assert status in ['failure', 'ok']
    return json.dumps({'status': status, 'reason': reason, 'result': result})

class HTTPPresence(resource.Resource):
    isLeaf = True
    def __init__(self, presence, users=None):
        self.presence = presence
        self.users = users or {}

    def _filterPath(self, path):
        return [x for x in path if x]

    def render_GET(self, request):
        debug("HTTP | Received GET request: %r" % request)
        path = self._filterPath(request.postpath)
        if len(path) == 1:
            return self.getPresence(request, path[0])
        elif len(path) == 0:
            if not self.authenticate(request):
                return response("failure", "Authentication required")
            return self.dumpAllPresence(request)
        return response("failure", "Invalid URI")

    def render_PUT(self, request):
        debug("HTTP | Received PUT request: %r" % request)
        if not self.authenticate(request):
            return response("failure", "Authentication required")
        path = self._filterPath(request.postpath)
        if len(path) == 1:
            return self.putPresence(request, path[0], request.content)
        if len(path) == 2:
            return self.putPresence(request, path[0], request.content, tag=path[1])
        return response("failure", "Invalid URI")

    def render_DELETE(self, request):
        debug("HTTP | Received DELETE request: %r" % request)
        if not self.authenticate(request):
            return response("failure", "Authentication required")
        path = self._filterPath(request.postpath)
        if len(path) == 2:
            return self.removePresence(request, path[0], path[1])
        return response("failure", "Invalid URI")

    def render_POST(self, request):
        debug("HTTP | Received POST request: %r" % request)
        if not self.authenticate(request):
            return response("failure", "Authentication required")
        path = self._filterPath(request.postpath)
        if path:
            return response("failure", "Invalid URI")
        return self.putAllStatuses(request, request.content)

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

    def getPresence(self, request, resource):
        def reply(presence):
            if presence:
                status = presence["status"]
            else:
                status = "offline"
            request.setHeader("X-Presence-Status", str(status))
            request.write(response("ok", "Success", {'presence': status}))
            request.finish()
        d = self.presence.get(resource)
        d.addCallback(reply)
        return server.NOT_DONE_YET

    def dumpAllPresence(self, request):
        raise NotImplementedError

    def putPresence(self, request, resource, content, tag=None):
        def reply(tag):
            request.write(response("ok", "Success", {'tag': tag}))
            request.finish()

        try:
            r = json.load(content)
        except ValueError, e:
            return response("failure", "Invalid data: " + str(e))
        if 'presence' not in r or 'status' not in r['presence']:
            return response("failure", "Presence status required")
        status = r['status']
        kw = {}
        if tag:
            kw['tag'] = tag
        if 'priority' in r:
            kw['priority'] = int(r['priority'])
        if 'expires' in r:
            kw['expires'] = int(r['expires'])
        d = self.presence.put(resource, status, **kw)
        d.addCallback(reply)
        d.addErrback(self._replyError, request)
        return server.NOT_DONE_YET

    def removePresence(self, request, resource, tag):
        def reply(r):
            if r:
                request.write(response("ok", "Success"))
            else:
                request.write(response("failure", "Not Found"))
            request.finish()

        d = self.presence.remove(resource, tag)
        d.addCallback(reply)
        return server.NOT_DONE_YET

    def putAllStatuses(self, request, content):
        def reply(r):
            request.write(response("ok", "Success"))
            request.finish()

        try:
            docs = json.load(content)
        except ValueError, e:
            return response("failure", str(e))
        dl = []
        for (resource, r) in docs.items():
            kw = {}
            #XXX: rework it
            status = r['presence']['status']
            expires = int(r['expires'])
            if 'priority' in r:
                kw['priority'] = int(r['priority'])
            if 'tag' in r:
                kw['tag'] = r['tag']
            dl.append(self.presence.put(resource, status, expires, **kw))
        d = defer.DeferredList(dl, fireOnOneErrback=True)
        d.addCallback(reply)
        d.addErrback(self._replyError, request)
        return server.NOT_DONE_YET

    def _replyError(self, failure, request):
        failure.trap(PresenceError)
        msg = failure.getErrorMessage()
        request.write(response("failure", msg))
        request.finish()

