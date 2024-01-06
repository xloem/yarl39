# Copyright 2024 Karl Semich
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

import concurrent.futures
import threading, queue
import time
import logging
logger = logging.getLogger(__name__)

class SyncThreadPump(threading.Thread):
    '''
    A Thread that autostarts when used as a context manager and passes work to
    send() in the background, attempting to maximally reach a defined accumulated
    size per period of time.
    
    Ongoing data can be passed to feed() and batches of results iterated with
    fetch() in order. Incidental data can be returned in a single call to immed()
    and will be moved toward the top of the queue to get a quick result.

    Passing size_per_period=None will measure capacity to determine the value.
    '''
    def __init__(self, send, size_per_period=1024000, period_secs=1):
        self.send = send
        self.period_size_limit = size_per_period
        self.period_secs = period_secs
        self.queue_bg_in = queue.Queue()
        self.queue_fg = queue.Queue()
        self.queue_bg_out = queue.Queue()
        self.data_event = threading.Event()
        super().__init__()
    def feed(self, size, *send_params, **send_kwparams):
        assert self.running
        fut = DeferredProxiedFuture()
        self.queue_bg_in.put([size, send_params, send_kwparams, fut])
        self.queue_bg_out.put(fut)
        return fut
    def fetch(self, ct):
        assert self.running or self.queue_bg_out.qsize() >= ct
        return (self.queue_bg_out.get().result() for x in range(ct))
    def immed(self, size, *send_params, **send_kwparams):
        assert self.running
        fut = DeferredProxiedFuture()
        self.queue_fg.put([size, send_params, send_kwparams, fut])
        self.data_event.set()
        return fut.result()
    def run(self):
        period_size_limit = self.period_size_limit
        period_size_best = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=128) as pool:
            mark_send = time.time()
            mark_send_next = mark_send + self.period_secs
            mark_recv = None
            mark_recv_next = None
            sys_futs = set()
            period_size_sent = 0
            period_size_returned = 0
            while True:
                self.data_event.clear()
                if self.queue_fg.qsize():
                    size, params, kwparams, user_fut = self.queue_fg.get_nowait()
                elif self.queue_bg_in.qsize():
                    size, params, kwparams, user_fut = self.queue_bg_in.get_nowait()
                elif not self.running:
                    break
                else:
                    self.data_event.wait(timeout=0.125)
                    continue
                now = time.time()
                if period_size_limit is not None and period_size_sent >= period_size_limit:
                    time.sleep(mark_send_next - now)
                    now = mark_send_next
                if now >= mark_send_next:
                    period_size_sent = 0
                    mark_send_next += self.period_secs
                    if now > mark_send_next:
                        mark_send_next = now + self.period_secs
                period_size_sent += size
                sys_fut = pool.submit(self.send, *params, **kwparams)
                sys_fut.size = size
                sys_fut.time = now
                user_fut.proxy(sys_fut)
                sys_futs.add(sys_fut)
                done, sys_futs = concurrent.futures.wait(
                    sys_futs,
                    timeout=0,
                    return_when=concurrent.futures.FIRST_COMPLETED
                )
                if done:
                    if mark_recv is None:
                        mark_recv = min([fut.time for fut in done])
                        mark_recv_next = mark_recv + self.period_secs
                    while True:
                        done_next_period = set([fut for fut in done if fut.time >= mark_recv_next])
                        done_this_period = done - done_next_period
                        period_size_returned += sum([fut.size for fut in done_this_period])

                        if not done_next_period:
                            break
                        
                        if period_size_returned > period_size_best:
                            period_size_best = period_size_returned
                            if self.period_size_limit is None:
                                # whatever we actually send is the max bandwidth
                                period_size_limit = period_size_best
                            elif period_size_best < self.period_size_limit:
                                import inspect
                                logger.warn(inspect.cleandoc(f'''-v
                                    Measured size_per_period underperforms passed size_per_period;
                                    this will slow calls to immed() from backpressure if it persists.
                                    Pass size_per_period=None to simply use measured value.
                                    measured={period_size_best}/{self.period_secs}
                                    passed={self.period_size_limit}/{self.period_secs}
                                '''))

                        mark_recv = mark_recv_next
                        mark_recv_next += self.period_secs
                        done = done_next_period
                        period_size_returned = 0
            # drain
            while len(sys_futs):
                done, sys_futs = concurrent.futures.wait(
                    sys_futs,
                    timeout=None,
                    return_when=concurrent.futures.FIRST_COMPLETED
                )
    def start(self):
        self.running = True
        return super().start()
    def join(self):
        self.running = False
        return super().join()
    def __enter__(self):
        self.start()
        return self
    def __exit__(self, e1, e2, e3):
        self.running = False
        if e1 is None:
            self.join()

class DeferredProxiedFuture(concurrent.futures.Future):
    def proxy(self, fut):
        fut.add_done_callback(self._proxy_result)
    def _proxy_result(self, fut):
        try:
            self.set_result(fut.result())
        except concurrent.futures.CancelledError:
            self.cancel()
        except Exception as e:
            self.set_exception(e)
