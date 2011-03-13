# -*- coding: utf-8 -*-

from random import choice
from string import ascii_letters

from twisted.internet import reactor

def random_str(len):
    return "".join(choice(ascii_letters) for x in xrange(len))

def presence_keyf(status):
    priority = status['priority']
    presence_status = 1 if status['status'] == 'online' else 0
    return 2 * priority + presence_status
