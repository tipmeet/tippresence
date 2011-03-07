# -*- coding: utf-8 -*-

import json

import utils

from twisted.internet import reactor, defer
from twisted.python import log

from tippresence import stats

def debug(msg):
    if __debug__:
        log.msg(msg)

def aggregate_status(statuses):
    max_priority = None
    aggr_presence = {'status': 'offline'}
    for tag, status in statuses:
        cur_priority = status['priority']
        if cur_priority > max_priority:
            max_priority = cur_priority
            aggr_presence = status['presence']
        elif max_priority == cur_priority and aggr_presence and aggr_presence['status'] == 'offline' and status['presence']['status'] == 'online':
            aggr_presence = status['presence']
    return {'presence': aggr_presence}

class Status(dict):
    def __init__(self, pdoc, expiresat, priority):
        dict.__init__(self)
        self['presence'] = pdoc
        self['expiresat'] = expiresat
        self['priority'] = priority

    def serialize(self):
        return json.dumps(self)

    @classmethod
    def parse(cls, s):
        r = json.loads(s)
        return cls(r['presence'], r['expiresat'], r['priority'])

class PresenceServiceError(Exception):
    pass

class PresenceService(object):
    MAX_EXPIRE_TIME = 3900
    table_aggregated_statuses = "sys:aggregated_statuses"

    def __init__(self, storage):
        storage.addCallbackOnConnected(self._loadStatusTimers)
        self.storage = storage
        self._callbacks = []
        self._status_timers = {}

    @defer.inlineCallbacks
    def putStatus(self, resource, pdoc, expires, priority=0, tag=None):
        if expires > self.MAX_EXPIRE_TIME:
            raise PresenceServiceError("Expire limit exeeded")
        if not tag:
            tag = utils.random_str(10)
        expiresat = expires + reactor.seconds()
        table = self._resourceTable(resource)
        rset = self._resourcesSet()
        status = Status(pdoc, expiresat, priority)
        d1 = self.storage.hset(table, tag, status.serialize())
        d2 = self.storage.sadd(rset, resource)
        d3 = self._notifyWatchers(resource)
        d4 = self._setStatusTimer(resource, tag, expires)
        yield defer.DeferredList([d1, d2, d3, d4])
        yield self._aggregate(resource)
        stats['presence_put_statuses'] += 1
        log.msg("Put status (resource: %r, tag: %r, presence document: %r, expires: %r, priority: %r) ==> result: ok" %\
                (resource, tag, pdoc, expires, priority))
        defer.returnValue(tag)

    @defer.inlineCallbacks
    def updateStatus(self, resource, tag, expires):
        r = yield self.getStatus(resource, tag)
        if r:
            _, status = r[0]
        else:
            defer.returnValue('not_found')
        expiresat = expires + reactor.seconds()
        status['expiresat'] = expiresat
        table = self._resourceTable(resource)
        d1 = self.storage.hset(table, tag, status.serialize())
        d2 = self._notifyWatchers(resource, status=r)
        d3 = self._setStatusTimer(resource, tag, expires)
        yield defer.DeferredList([d1, d2, d3])
        stats['presence_updated_statuses'] += 1
        log.msg("Update status (resource: %r, tag: %r, expires: %r) ==> result: ok" % (resource, tag, expires))

    @defer.inlineCallbacks
    def getStatus(self, resource, tag=None):
        stats['presence_gotten_statuses'] += 1
        table = self._resourceTable(resource)
        try:
            if tag:
                r = yield self.storage.hget(table, tag)
                r = {tag: r}
            else:
                r = yield self.storage.hgetall(table)
        except KeyError:
            log.msg("Get status (resource: %r, tag: %r) ==> result: not found" % (resource, tag))
            defer.returnValue([])
        statuses = [(t, Status.parse(x)) for (t,  x) in r.iteritems()]
        active, expired = self._splitExpiredStatuses(statuses)
        if expired:
            debug("Get status (resource: %r, tag: %r) ==> expired statuses found: %r" % (resource, tag, expired))
            for t, _ in expired:
                self.removeStatus(resource, t)
        log.msg("Get status (resource: %r, tag: %r) ==> result: %r" % (resource, tag, active))
        defer.returnValue(active)

    @defer.inlineCallbacks
    def dumpStatuses(self):
        rset = self._resourcesSet()
        all_resources = yield self.storage.sgetall(rset)
        result = {}
        debug("Dump all statuses...")
        for resource in all_resources:
            result[resource] = yield self.getStatus(resource)
        stats['presence_dumped_statuses'] += 1
        defer.returnValue(result)

    @defer.inlineCallbacks
    def removeStatus(self, resource, tag):
        stats['presence_removed_statuses'] += 1
        table = self._resourceTable(resource)
        yield self._cancelStatusTimer(resource, tag)
        try:
            yield self.storage.hdel(table, tag)
        except KeyError, e:
            log.msg("Remove status (resource: %r, tag: %r) ==> result: not found" % (resource, tag))
            defer.returnValue("not_found")
        try:
            yield self.storage.hgetall(table)
        except KeyError:
            rset = self._resourcesSet()
            yield self.storage.srem(rset, resource)
        yield self._notifyWatchers(resource)
        log.msg("Remove status (resource: %r, tag: %r) ==> result: ok" % (resource, tag))
        defer.returnValue("ok")

    def watch(self, callback, *args, **kwargs):
        self._callbacks.append((callback, args, kwargs))

    @defer.inlineCallbacks
    def _aggregate(self, resource):
        statuses = yield self._getStatuses(resource)
        statuses = yield self._removeExpiredStatuses(resource, statuses)
        if not statuses:
            debug("Aggregate status for resource %r. No statuses found.")
        else:
            aggregated_status = self._aggregateStatuses(statuses)
            yield self.storage.hset(self.table_aggregated_statuses, resource, aggregated_status[1].serialize())

    def _aggregateStatuses(self, statuses):
        result = max(statuses, key=lambda x: utils.status_keyf(x[1]))
        debug("Aggregate statuses %r => %r" % (statuses, result))
        return result

    @defer.inlineCallbacks
    def _getStatuses(self, resource):
        table = self._resourceTable(resource)
        try:
            r = yield self.storage.hgetall(table)
        except KeyError:
            debug("Get statuses for resource %r: no statuses found" % resource)
            defer.returnValue([])
        statuses = [(tag, Status.parse(x)) for (tag,  x) in r.iteritems()]
        debug("Get statuses for resource %r: %r" % (resource, statuses))
        defer.returnValue(statuses)

    def _removeExpiredStatuses(self, resource, statuses):
        active, expired = self._splitExpiredStatuses(statuses)
        debug("Remove expired statuses for %r. All statuses %r" % (resource, statuses))
        if expired:
            debug("Remove expired statuses for %r. Expired statuses found %r. Cleaning..." % (resource, expired))
            for tag, _ in expired:
                self.removeStatus(resource, tag)
        debug("Remove expired statuses for %r. Active statuses %r" % (resource, active))
        return active

    def _splitExpiredStatuses(self, statuses):
        active = []
        expired = []
        cur_time = reactor.seconds()
        for tag, status in statuses:
            if status['expiresat'] < cur_time:
                expired.append((tag, status))
            else:
                active.append((tag, status))
        return active, expired

    @defer.inlineCallbacks
    def _setStatusTimer(self, resource, tag, delay, memonly=False):
        if (resource, tag) in self._status_timers:
            self._status_timers[resource, tag].reset(delay)
        else:
            stats['presence_active_timers'] += 1
            self._status_timers[resource, tag] = reactor.callLater(delay, self.removeStatus, resource, tag)
        if not memonly:
            yield self._storeStatusTimer(resource, tag, delay)
        debug("Set status timer (resource: %r, tag: %r, delay: %r) ==> result: ok" % (resource, tag, delay))

    @defer.inlineCallbacks
    def _cancelStatusTimer(self, resource, tag):
        if (resource, tag) in self._status_timers:
            stats['presence_active_timers'] -= 1
            timer = self._status_timers.pop((resource, tag))
            if timer.active():
                timer.cancel()
            yield self._dropStatusTimer(resource, tag)
            debug("Cancel status timer (resource: %r, tag: %r) ==> result: ok" % (resource, tag))
        else :
            debug("Cancel status timer (resource: %r, tag: %r) ==> result: not found" % (resource, tag))

    @defer.inlineCallbacks
    def _storeStatusTimer(self, resource, tag, delay):
        table = self._timersTable()
        key = '%s:%s' % (resource, tag)
        expiresat = reactor.seconds() + delay
        yield self.storage.hset(table, key, expiresat)
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
        for callback, arg, kw in self._callbacks:
            callback(resource, status, *arg, **kw)

    def _resourceTable(self, resource):
        return 'res:' + resource

    def _timersTable(self):
        return 'sys:status_timers'

    def _resourcesSet(self):
        return 'sys:resources'

