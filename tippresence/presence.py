# -*- coding: utf-8 -*-

import json

import utils

from twisted.internet import reactor, defer
from twisted.python import log

def debug(msg):
    if __debug__:
        log.msg(msg)

def calc_expires_at(expires):
    return reactor.seconds() + expires

class PresenceError(Exception):
    pass


class PresenceService(object):
    MAX_EXPIRES = 3900
    DEFAULT_EXPIRES = 3600
    allowed_statuses = ["online", "offline"]
    _key_presence = "presence:%s:%s"
    _key_resource_presence = "resource_presence:%s"
    _key_resources = "resources"

    def __init__(self, storage):
        storage.addCallbackOnConnected(self._loadStatusTimers)
        self.storage = storage
        self._watch_callbacks = []
        self._expires_timers = {}
        self.stats_put = 0
        self.stats_update = 0
        self.stats_get = 0
        self.stats_remove = 0

    @defer.inlineCallbacks
    def put(self, resource, status, expires=DEFAULT_EXPIRES, priority=0, tag=None):
        debug("PUT | %s:%s | Received put request: resource %r, status %r, expires %r, priority %r, tag %r" %\
                (resource, tag, resource, status, expires, priority, tag))
        self.stats_put += 1
        if not tag:
            tag = utils.random_str(10)
            debug("PUT | %s:%s | Generate tag for presence: %r." % (resource, tag, tag))
        if expires > self.MAX_EXPIRES:
            debug("PUT | %s:%s | Max expires time exceeded. Requested %r, allowed %r. Raise exception." %\
                    (resource, tag, expires, self.MAX_EXPIRES))
            raise PresenceError("Expire limit exeeded")
        if status not in self.allowed_statuses:
            debug("PUT | %s:%s | Unknown status value: %r. Allowed statuses: %r. Raise exception." %\
                    (resource, tag, status, self.allowed_statuses))
            raise PresenceError("Unknown status value: %r. Allowed: %r" % (status, self.allowed_statuses))
        presence = {"resource": resource, "tag": tag, "status": status, "expires": expires, "priority": priority}
        yield self._storePresence(resource, tag, presence)
        self._setExpireTimer(resource, tag, expires)
        self._notifyWatchers(resource)
        debug("PUT | %s:%s | Put presence for resource %r with tag %r: %r" %\
                (resource, tag, resource, tag, presence))
        defer.returnValue(tag)

    @defer.inlineCallbacks
    def update(self, resource, tag, expires):
        debug("UPDATE | %s:%s | Received update request: resource %r, tag %r, expires %r" %\
                (resource, tag, resource, tag, expires))
        self.stats_update += 1
        if expires > self.MAX_EXPIRES:
            debug("UPDATE | %s:%s | Max expires time exceeded. Requested %r, allowed %r. Raise exception." %\
                    (resource, tag, expires, self.MAX_EXPIRES))
            raise PresenceError("Expire limit exceeded")
        r = yield self._updatePresenceExpires(resource, tag, expires)
        if r:
            self._updateExpireTimer(resource, tag, expires)
            debug("UPDATE | %s:%s | Update presence for resource %r with tag %r: expires %r" %\
                    (resource, tag, resource, tag, expires))
            defer.returnValue(1)
        debug("UPDATE | %s:%s | Update failed.")

    @defer.inlineCallbacks
    def get(self, resource, tag=None):
        debug("GET | %s:%s | Received get request: resource %r, tag %r" %\
                (resource, tag, resource, tag))
        self.stats_get += 1
        presence = None
        if tag:
            presence = yield self._getPresence(resource, tag)
            debug("GET | %s:%s | Loaded presence for tag: %r" % (resource, tag, presence))
        else:
            presence_list = yield self._getAllPresence(resource)
            debug("GET | %s:%s | Dumped all presence information: %r" % (resource, tag, presence_list))
            if presence_list:
                presence = max(presence_list, key=utils.presence_keyf)
                debug("GET | %s:%s | Aggregated presence: %r" % (resource, tag, presence))
        if presence:
            debug("GET | %s:%s | Presence for resource %r with tag %r: %r." %\
                    (resource, tag, resource, tag, presence))
            defer.returnValue(presence)
        debug("GET | %s:%s | Presence for resource %r with tag %r not found." %\
                (resource, tag, resource, tag))

    def dump(self):
        raise NotImplementedError

    @defer.inlineCallbacks
    def remove(self, resource, tag):
        log.msg("REMOVE | %s:%s | Received remove request: resource %r, tag %r" %\
                (resource, tag, resource, tag))
        self.stats_remove += 1
        r = yield self._removePresence(resource, tag)
        if r:
            self._cancelExpireTimer(resource, tag)
            self._notifyWatchers(resource)
            log.msg("REMOVE | %s:%s | Removed presence for resource %r with tag %r" %\
                    (resource, tag, resource, tag))
            defer.returnValue(1)
        log.msg("REMOVE | %s:%s | Presence for resource %r with tag %r not found." %\
                (resource, tag, resource, tag))

    def watch(self, callback, *args, **kwargs):
        self._watch_callbacks.append((callback, args, kwargs))

    @defer.inlineCallbacks
    def _storePresence(self, resource, tag, presence):
        expires = presence['expires']
        expires_at = calc_expires_at(expires)
        presence["expires_at"] = expires_at
        key = self._key_presence % (resource, tag)
        debug("STORE | %s:%s | Store presence %r for key %r" % (resource, tag, presence, key))
        yield self.storage.hsetn(key, presence)
        resource_presence_key = self._key_resource_presence % resource
        debug("STORE | %s:%s | Add tag %r to presence list (key %r)" %\
                (resource, tag, tag, resource_presence_key))
        yield self.storage.sadd(resource_presence_key, tag)
        debug("STORE | %s:%s | Add resource %r to resources list (key %r)" %\
                (resource, tag, resource, self._key_resources))
        yield self.storage.sadd(self._key_resources, resource)

    @defer.inlineCallbacks
    def _updatePresenceExpires(self, resource, tag, expires):
        expires_at = calc_expires_at(expires)
        key = self._key_presence % (resource, tag)
        try:
            yield self.storage.hget(key, "tag")
        except KeyError:
            debug("STORE | %s:%s | Caught KeyError exception from storage backend. Presence not found." %\
                    (resource, tag))
            defer.returnValue(None)
        debug("STORE | %s:%s | Update expires to %r (expires at %r) for key %r" %\
                (resource, tag, expires, expires_at, key))
        yield self.storage.hset(key, "expires", expires)
        yield self.storage.hset(key, "expires_at", expires_at)
        defer.returnValue(1)

    @defer.inlineCallbacks
    def _getPresence(self, resource, tag):
        key = self._key_presence % (resource, tag)
        try:
            presence = yield self.storage.hgetall(key)
            presence['expires'] = int(presence['expires'])
            presence['expires_at'] = float(presence['expires_at'])
            presence['priority'] = int(presence['priority'])
        except KeyError:
            debug("STORE | %s:%s | Caught KeyError exception for key %r. Presence not found." %\
                    (resource, tag, key))
            defer.returnValue(None)
        debug("STORE | %s:%s | Gotten presence for resource %r with tag %r: %r." %\
                (resource, tag, resource, tag, presence))
        defer.returnValue(presence)

    @defer.inlineCallbacks
    def _removePresence(self, resource, tag):
        key = self._key_presence % (resource, tag)
        resource_presence_key = self._key_resource_presence % resource
        try:
            yield self.storage.hdrop(key)
        except KeyError:
            debug("STORE | %s:%s | Caught KeyError exception for key %r. Presence not found." %\
                    (resource, tag, key))
        else:
            debug("STORE | %s:%s | Remove tag %r from presence list of resource %r." %\
                    (resource, tag, tag, resource))
            yield self.storage.srem(resource_presence_key, tag)
            debug("STORE | %s:%s | Removed presence for resource %r with tag %r." %\
                    (resource, tag, resource, tag))
            defer.returnValue(1)

    @defer.inlineCallbacks
    def _getAllPresence(self, resource):
        resource_presence_key = self._key_resource_presence % resource
        try:
            tags = yield self.storage.sgetall(resource_presence_key)
        except KeyError:
            debug("STORE | %s | Caught KeyError exception for key %r. Resource not found." %\
                    (resource, resource_presence_key))
            defer.returnValue(None)
        debug("STORE | %s | Gotten tags for resource %r: %r" %\
                (resource, resource, tags))
        presence_list = []
        for tag in tags:
            presence = yield self._getPresence(resource, tag)
            if not presence:
                debug("STORE | %s | Faield to get presence for resource %r with tag %r." %\
                        (resource, resource, tag))
            else:
                presence_list.append(presence)
        if presence_list:
            debug("STORE | %s:%s | Gotten all presence for resource %r: %r." %\
                    (resource, tag, resource, presence_list))
            defer.returnValue(presence_list)

    def _setExpireTimer(self, resource, tag, expires):
        tid = reactor.callLater(expires, self._expireTimerCb, resource, tag)
        self._expires_timers[resource, tag] = tid
        debug("TIMER | %s:%s | Timer is set to %r seconds" % (resource, tag, expires))

    @defer.inlineCallbacks
    def _expireTimerCb(self, resource, tag):
        debug("TIMER | %s:%s | Executed presence expire callback. Remove expired presence." %\
                (resource, tag))
        yield self._removePresence(resource, tag)
        self._notifyWatchers(resource)

    def _updateExpireTimer(self, resource, tag, expires):
        tid = self._expires_timers.get((resource, tag))
        if not tid:
            raise PresenceError("Timer not found. Update faield.")
        tid.reset(expires)
        debug("TIMER | %s:%s | Timer is updated to %r seconds." % (resource, tag, expires))

    def _cancelExpireTimer(self, resource, tag):
        tid = self._expires_timers.get((resource, tag))
        if not tid:
            raise PresenceError("Timer not found. Cancel faield.")
        tid.cancel()
        self._expires_timers.pop((resource, tag))
        debug("TIMER | %s:%s | Timer is canceled." % (resource, tag))

    @defer.inlineCallbacks
    def _recoverExpireTimers(self):
        debug("TIMER_RECOVER | Recover timers..")
        resources = yield self.storage.sgetall(self._key_resources)
        debug("TIMER_RECOVER | Received resources list: %r." % resources)
        for resource in resources:
            debug("TIMER_RECOVER | Recover timers for resource %r." % resource)
            presence_list = yield self._getAllPresence(resource)
            if not presence_list:
                debug("TIMER_RECOVER | Presence for resource %r not found. Go ahead..." % resource)
                continue
            for presence in presence_list:
                tag = presence['tag']
                expires_at = presence['expires_at']
                expires = expires_at - reactor.seconds()
                debug("TIMER_RECOVER | Recover timer for resource %r with tag %r." % (resource, tag))
                if expires < 0:
                    debug("TIMER_RECOVER | Presence %r expired." % presence)
                    expires = 0
                self._setExpireTimer(resource, tag, expires)
        debug("TIMER_RECOVER | Done.")

    @defer.inlineCallbacks
    def _notifyWatchers(self, resource):
        debug("NOTIFY | %s | Notify watchers about resource %r presence." % (resource, resource))
        presence_list = yield self._getAllPresence(resource)
        if not presence_list:
            self._sendPresence(resource, None)
            defer.returnValue(None)
        presence = max(presence_list, key=utils.presence_keyf)
        self._sendPresence(resource, presence)

    def _sendPresence(self, resource, presence):
        debug("NOTIFY | %s | Send presence %r of resource %r to all watchers." % (resource, presence, resource))
        for callback, arg, kw in self._watch_callbacks:
            callback(resource, presence, *arg, **kw)

