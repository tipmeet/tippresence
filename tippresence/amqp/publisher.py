# -*- coding: utf-8 -*-

import json

from twisted.internet import defer, protocol

from pkg_resources import resource_filename

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

SPECFILE = resource_filename(__name__, 'amqp0-8.xml')

class AMQPublisher(object):
    exchange_name = ''
    routing_key = 'presence_changes'

    def __init__(self, factory, presence_service):
        presence_service.watch(self.presenceChanged)
        self.factory = factory

    @defer.inlineCallbacks
    def presenceChanged(self, resource, presence):
        if presence:
            status = presence['status']
        else:
            status = "offline"
        msg = json.dumps([resource, {'presence': {'status': status}}])
        yield self.factory.publish(self.exchange_name, msg, self.routing_key)


class AMQFactory(protocol.ReconnectingClientFactory):
    VHOST = '/'

    def __init__(self, creds):
        self.spec = txamqp.spec.load(SPECFILE)
        self.creds = creds
        self.client = None
        self.channel  = None

    def buildProtocol(self, addr):
        self.resetDelay()
        delegate = TwistedDelegate()
        self.client = AMQClient(delegate=delegate, vhost=self.VHOST, spec=self.spec)
        self.client.start(self.creds)
        return self.client

    @defer.inlineCallbacks
    def publish(self, exchange, msg, routing_key):
        if not self.client:
            raise NotImplementedError
        if not self.channel:
            yield self._createChannel()
        content = Content(msg)
        yield self.channel.basic_publish(exchange=exchange, content=content, routing_key=routing_key)

    @defer.inlineCallbacks
    def _createChannel(self):
        self.channel = yield self.client.channel(1)
        yield self.channel.channel_open()

