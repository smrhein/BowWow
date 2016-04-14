'''
Created on Apr 12, 2014

@author: rhein
'''
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import skimage.io as skio
import time
import multiprocessing as mp
import os
import multiprocessing.managers
import traceback
import sys
import threading
import httplib
import StringIO
import Image
import math
import Queue

from multiprocessing.pool import ThreadPool as Pool
from Queue import PriorityQueue as LocalPriorityQueue
from Queue import PriorityQueue as RemotePriorityQueue
from threading import Lock

# from multiprocessing.pool import Pool
# from Queue import PriorityQueue as LocalPriorityQueue
# type(mp.Manager()).register('PriorityQueue', Queue.PriorityQueue)
# RemotePriorityQueue = mp.Manager().PriorityQueue
# from multiprocessing import Lock
    
class DummyResult(object):
    def __init__(self, value):
        self._value = value
    def get(self, timeout=None):
        return self._value
    def ready(self):
        return True
    def successful(self):
        return True
    def wait(self, timeout=None):
        pass

def mp_print(*args):
    sys.stderr.write('[{}:{}] {}\n'.format(os.getpid(), threading.current_thread().ident, *args))

def init_conn():
    conns = globals()
    conn = httplib.HTTPConnection('bowwow116.viewmydog.com')
    conn.set_debuglevel(0)
    conns[(os.getpid(), threading.current_thread().ident)] = conn
    
def produce(tokenq):
    t = time.time()
    conns = globals()
    try:
        success = True
        with lck:
            token = tokenq.get_nowait()
            conn = conns[(os.getpid(), threading.current_thread().ident)]
            try:
                conn.request("GET", '/cam{}.jpg/'.format(camid))                
            except:
                success = False
        if success:
            r1 = conn.getresponse()
            data = r1.read()
            buff = StringIO.StringIO()
            buff.write(data)
            buff.seek(0)
            pim = Image.open(buff)
            frame = np.asarray(pim)
#             frame = .3 * frame[..., 0] + .6 * frame[..., 1] + .1 * frame[..., 2]
    #         mp_print('{} SENDING  {:2.2f}'.format(token, 1. / (time.time() - t)))
        else:
            conn.close()
            conn.connect()
            raise
        return token, frame
    except:
#         traceback.print_exc()
        raise

lastframe = time.time()
def consume(frameid, pool, resq, tokenq):
    global lastframe
    try:
        try:
            tokenq.put_nowait(frameid)
            res = pool.apply_async(produce, (tokenq,))
            resq.put_nowait((frameid, id(res), res))
        except Queue.Full:
            mp_print('{} DROPPED'.format(frameid))
        
        try:
            frameid, _, res = resq.get_nowait()            
            if res.ready() and res.successful():
                token, frame = res.get(0)
                if token == frameid:
                    im.set_array(frame)            
                    mp_print('{} RECEIVED {:2.2f}'.format(frameid, 1. / (time.time() - lastframe)))
                    lastframe = time.time()
                else:
                    mp_print('{} MISMATCH {}'.format(frameid, token))
                    res = DummyResult((token, frame))
                    resq.put_nowait((token, _, res))
            elif res.ready():
                try:
                    res.get(0)
                except Exception, e:
                    mp_print('{} CORRUPT  {}'.format(frameid, e))                
            else:
                mp_print('{} DELAYED'.format(frameid))
                resq.put_nowait((frameid, _, res))
        except Queue.Empty:
            mp_print('{} UNCLEAR'.format(frameid))                     
    except:
        traceback.print_exc()
    finally:
        return im,
    
def main():    
    pool = Pool(int(math.ceil(2 * fps)), init_conn)
    resq = LocalPriorityQueue()
    tokenq = RemotePriorityQueue(2 * fps)
    _ = animation.FuncAnimation(fig, consume, interval=1000. / fps, fargs=(pool, resq, tokenq,), blit=True)
    plt.show()

lck = Lock()
    
if __name__ == '__main__':
    plt.switch_backend('TkAgg')
    fps = 2
    camid = 6    
    fig = plt.figure()
    frame = skio.imread('http://bowwow116.viewmydog.com/cam{}.jpg/'.format(camid))
#     frame = .3 * frame[..., 0] + .6 * frame[..., 1] + .1 * frame[..., 2]
    im = plt.imshow(frame, cmap='gray')
    main()    
