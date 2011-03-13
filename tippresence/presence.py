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
    ht_presence = "presence:%s:%s"
    set_resource_presence = "set:presence:%s"
    ht_aggregated_presence = "aggregated_presence:%s"
    allowed_statuses = ["online", "offline"]

    def __init__(self, storage):
        storage.addCallbackOnConnected(self._loadStatusTimers)
        self.storage = storage
        self._watch_callbacks = []
        self._status_timers = {}
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
        self.stats_update += 1
        expires_at = expires + reactor.seconds()
        yield self._updateExpiresAt(resource, tag, expires_at)
        log.msg("Update presence for resource %r (tag: %r): expires: %r." % (resource, tag, expires))

    @defer.inlineCallbacks
    def get(self, resource, tag=None):
        self.stats_get += 1
        if tag:
            presence = yield self._getPresence(resource, tag)
            defer.returnValue(presence)
        presence = yield self._getAggregatedPresence(resource)
        log.msg("Get presence for resource %r: %r." % (resource, presence))
        defer.returnValue(presence)

    def dump(self):
        raise NotImplementedError

    @defer.inlineCallbacks
    def remove(self, resource, tag):
        self.stats_remove += 1
        yield self._removePresence(resource, tag)
        yield self._aggregatePresence(resource)
        log.msg("Remove presence for resource %r (tag %r)" % (resource, tag))

    def watch(self, callback, *args, **kwargs):
        self._watch_callbacks.append((callback, args, kwargs))

    @defer.inlineCallbacks
    def _aggregatePresence(self, resource):
        defer.returnValue(None)
        statuses = yield self._getAllStatuses(resource)
        if not statuses:
            debug("Aggregate presence for resource %r. Nothing to do.")
        else:
            aggregated_status = self._aggregateStatuses(statuses)
            yield self.storage.hset(self.table_aggregated_statuses, resource, aggregated_status[1].serialize())

    def _aggregateStatuses(self, presence):
        aggregated = max(statuses, key=utils.status_keyf)
        debug("Aggregate presence %r => %r" % (presence, aggregated))
        return aggregated

    @defer.inlineCallbacks
    def _getPresence(self, resource, tag):
        table = self.ht_presence % (resource, tag)
        try:
            presence = yield self.storage.hgetall(table)
        except KeyError:
            debug("Get presence for resource %r, tag %r: not found" % (resource, tag))
            raise PresenceNotFound("No presence for resource %r and tag %r found" % (resource, tag))
        debug("Get presence for resource %r, tag %r: %r" % (resource, tag, presence))
        defer.returnValue(presence)

    @defer.inlineCallbacks
    def _getAggregatedPresence(self, resource):
        table = self.ht_aggregated_presence % resource
        try:
            presence = yield self.storage.hgetall(table)
        except KeyError:
            log.msg("Get presence for resource %r: not found" % resource)
            raise PresenceNotFound("No presence for resource %r found" % resource)
        defer.returnValue(presence)

    @defer.inlineCallbacks
    def _storePresence(self, resource, tag, presence):
        presence_table = self.ht_presence % (resource, tag)
        presence_set = self.set_resource_presence % resource
        yield self.storage.hsetn(presence_table, presence)
        yield self.storage.sadd(presence_set, tag)

    @defer.inlineCallbacks
    def _removePresence(self, resource, tag):
        presence_table = self.ht_presence % (resource, tag)
        presence_set = self.set_resource_presence % resource
        yield self.storage.srem(presence_set, tag)
        yield self.storage.hdrop(presence_table)

    @defer.inlineCallbacks
    def _updateExpiresAt(self, resource, tag, expires_at):
        presence_table = self.ht_presence % (resource, tag)
        yield self.storage.hset(presence_table, "expires_at", expires_at)

    @defer.inlineCallbacks
    def _setExpireTimer(self, resource, tag, delay, memonly=False):
        if (resource, tag) in self._status_timers:
            self._status_timers[resource, tag].reset(delay)
        else:
            self._status_timers[resource, tag] = reactor.callLater(delay, self.removeStatus, resource, tag)
        if not memonly:
            yield self._storeStatusTimer(resource, tag, delay)
        debug("Set status timer (resource: %r, tag: %r, delay: %r) ==> result: ok" % (resource, tag, delay))

    @defer.inlineCallbacks
    def _cancelExpireTimer(self, resource, tag):
        if (resource, tag) in self._status_timers:
            timer = self._status_timers.pop((resource, tag))
            if timer.active():
                timer.cancel()
            yield self._dropStatusTimer(resource, tag)
            debug("Cancel status timer (resource: %r, tag: %r) ==> result: ok" % (resource, tag))
        else :
            debug("Cancel status timer (resource: %r, tag: %r) ==> result: not found" % (resource, tag))

    @defer.inlineCallbacks
    def _storeStatusTimer(self, resource, tag, delay):
        key = '%s:%s' % (resource, tag)
        expiresat = reactor.seconds() + delay
        yield self.storage.hset(self.ht_expire_timers, key, expiresat)
        debug("Store status timer to storage (resource: %r, tag: %r, delay: %r) ==> result: ok" %\
                (resource, tag, delay))

    @defer.inlineCallbacks
    def _dropStatusTimer(self, resource, tag):
        table = self._timersTable()
        key = '%s:%s' % (resource, tag)
        yield self.storage.hdel(table, key)
        debug("Remove status timer from storage (resource: %r, tag: %r) ==> result: ok" %\
                (resource, tag))

    @defer.inlineCallbacks
    def _loadStatusTimers(self):
        table = self._timersTable()
        debug("Start loading status timers")
        try:
            timers = yield self.storage.hgetall(table)
        except KeyError:
            defer.returnValue(None)
        stale_timers = []
        cur_time = reactor.seconds()
        for key, expiresat in timers.iteritems():
            resource, tag = key.split(':')
            expiresat = float(expiresat)
            if expiresat < cur_time:
                debug("Load status timers from storage (resource: %r, tag: %r, expires at: %r) ==> expired" %\
                    (resource, tag, expiresat))
                self.removeStatus(resource, tag)
            else:
                delay = expiresat - cur_time
                debug("Load status timers from storage (resource: %r, tag: %r, expires at: %r) ==> set timer" %\
                    (resource, tag, expiresat))
                yield self._setStatusTimer(resource, tag, delay, memonly=True)
        debug("Loading status timers ==> ok")

    @defer.inlineCallbacks
    def _notifyWatchers(self, resource, status=None):
        if not status:
            status = yield self.getStatus(resource)
        for callback, arg, kw in self._watch_callbacks:
            callback(resource, status, *arg, **kw)

    def _resourceTable(self, resource):
        return 'res:' + resource

