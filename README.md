# yarl39

Yet Another Rate Limiter 39

This is a simple sync & thread -based python package to pipeline dense calls of
an arbitrary function to meet and push a size-per-time constraint.

A background function `feed(size, *params, **kwparams)` is provided to queue up
many calls.

A foreground function `immed(size, *params, **kwparams)` is provided to bump
calls to the front of the queue for incidental immediate results.

Simple usage:
```
import yarl39

import requests
session = requests.Session()

with yarl39.SyncThreadPump(session.request, size_per_period = 1000000, period_secs = 1) as pump:
    for data in data_source:
        # feed() absorbs the size parameter, and hands the rest to session.request
        fut = pump.feed(size=len(data), method='POST', data=data)
    # on closure of the context, the pump waits for all data to complete, or it can be iterated with .fetch(ct)
```
