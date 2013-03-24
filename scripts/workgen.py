#!/usr/bin/env python2.7

import argparse
import comm
import datetime
import json
import os
import socket
import string
import struct
import sys
import time
import threading

def positive_int(value):
  ivalue = int(value)
  if ivalue <= 0:
      raise argparse.ArgumentTypeError("%s is not a positive int value" % value)
  return ivalue

def hostport(value):
  host, sport = string.split(value, ":", 1)
  return (host, positive_int(sport))

parser = argparse.ArgumentParser(description="Create work from file")
parser.add_argument("--verbose",
    help="Verbose output",
    action='store_true')
parser.add_argument("address",
    help="Address of master",
    type=hostport)
parser.add_argument("tracefile", nargs="?", type=argparse.FileType('r'),
    default=sys.stdin, help="Trace file as a stream of newline delimited json objects")

args = parser.parse_args()


class TraceJob:
  def __init__(self, job_id, descr):
    self.id = job_id;
    self.descr = descr;
    self.latency = 0;
    self.actual_resp = '';
    self.success = False;

    
class ConnectionPool(object):
  def __init__(self, hostport):
    self.hostport = hostport
    self.conns = []
    self.lock = threading.Lock()

  def get_conn(self):
    self.lock.acquire()
    ret = None
    if len(self.conns) > 0:
      ret = self.conns.pop()
    self.lock.release()

    if not ret:
      ret = socket.create_connection(self.hostport)
      if ret is None:
	print "Could not connect to %s" % self.hostport
	os._exit(1)

    return ret

  def put_conn(self, conn):
    self.lock.acquire()
    self.conns.append(conn)
    self.lock.release()

class WorkGenThread(threading.Thread):
  
    def __init__(self, conn_pool, job):
        threading.Thread.__init__(self)
        self.job = job;
	self.conn_pool = conn_pool

    def log(self, msg):
        #if args.verbose:
        print msg

    def validate_response(self, actual_resp, correct_resp):
        return (actual_resp == correct_resp);
      
    def run(self):
	sock = self.conn_pool.get_conn()
	before = datetime.datetime.now()

        comm.TaggedMessage(comm.WORK, 0).to_socket(sock)
	comm.send_string(sock, self.job.descr['work'])
	comm.TaggedMessage.from_socket(sock)
        self.job.actual_resp = comm.recv_string(sock)

	after = datetime.datetime.now()
        self.job.latency = after - before

        self.log("Request %d: req: \"%s\", resp: \"%s\", latency: %d ms" % (self.job.id, self.job.descr['work'], self.job.actual_resp, 1000 * self.job.latency.total_seconds()))
        
	self.conn_pool.put_conn(sock)
        
        # validation
        self.job.success = self.validate_response(self.job.actual_resp, self.job.descr['resp'])
        if not self.job.success:
            self.log("ERROR: incorrect response to req %d: (req: %s)" % (self.job.id, self.job.descr['work']))
            self.log("       expected: '%s'" % self.job.descr['resp']);
            self.log("       received: '%s'" % self.job.actual_resp);


######################################################################
            
conn_pool = ConnectionPool(args.address)

######################################################################
# wait until the server is ready before sending requests
######################################################################

print "Waiting for server to initialize...";
server_ready = False
while not server_ready:
  sock = conn_pool.get_conn()
  
  comm.TaggedMessage(comm.ISREADY, 0).to_socket(sock)
  comm.TaggedMessage.from_socket(sock)
  resp = comm.recv_string(sock)
  if resp == "ready":
    server_ready = True;
  else:
    time.sleep(0.5);

######################################################################
# Server is ready. Start the trace
######################################################################
    
print "Server ready, beginning trace...";
    
start_time = datetime.datetime.now()
traceJobs = []

def all_threads():
  # populate traceJobs
  for line in args.tracefile:
    traceJobs.append(TraceJob(len(traceJobs), json.loads(line)));

  for job in traceJobs:
    test_time = start_time + datetime.timedelta(milliseconds=job.descr['time'])

    while datetime.datetime.now() < test_time:
      sleep_time = (test_time - datetime.datetime.now()).total_seconds()
      # print "Sleeping %f" % (test_time - datetime.datetime.now()).total_seconds();
      if sleep_time > 0:
        time.sleep(sleep_time)

    if args.verbose:
      print "Starting thread", job.descr;
      
    t = WorkGenThread(conn_pool, job);
    t.start()
    yield t

    
threads = [t for t in all_threads()]
for thread in threads:
  thread.join()

end_time = datetime.datetime.now()
elapsed_time = (end_time - start_time).total_seconds();

######################################################################
# Trace is done, so now report results
######################################################################

print ""
print "--- Results Summary ---" 
print ""

avg_latency = 0.0;
jobs_counted = 0;
for job in traceJobs:
  if job.success:
    success_str = 'YES';
  else:
    success_str = 'NO';

  if job.descr['work'] != "cmd=lastrequest":
    avg_latency += 1000 * job.latency.total_seconds();
    jobs_counted += 1;  
    print "[%d] Request: %s, success: %s, latency: %d" % (job.id, job.descr['work'], success_str, 1000 * job.latency.total_seconds());

avg_latency = avg_latency / jobs_counted;
print ""
print "Avg request latency: %.2f ms" % avg_latency;
print "Total test time %.2f sec" % elapsed_time;
print ""

