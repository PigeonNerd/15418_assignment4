// Copyright 2013 15418 Course Staff.

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>
#include "server/messages.h"
#include "server/worker.h"
#include "tools/work_queue.h"

//#include <cstring>
//#include <iostream>

struct Worker_state {
    WorkQueue<Request_msg> cpu_work_queue;    
    WorkQueue<Request_msg> disk_work_queue;    
} wstate;


// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

// Implements logic required by primerange command for the request
// 'req' using multiple calls to execute_work.  This function fills in
// the appropriate response.
static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    for (int i=0; i<4; i++) {
      Request_msg dummy_req(0);
      Response_msg dummy_resp(0);
      create_computeprimes_req(dummy_req, params[i]);
      execute_work(dummy_req, dummy_resp);
      counts[i] = atoi(dummy_resp.get_response().c_str());
    }

    if (counts[1]-counts[0] > counts[3]-counts[2])
      resp.set_response("There are more primes in first range.");
    else
      resp.set_response("There are more primes in second range.");
}

void* executeWork_cpu(void* arg) {
    while(1) {
      Request_msg req = wstate.cpu_work_queue.get_work();
      Response_msg resp(req.get_tag());
      if (req.get_arg("cmd").compare("compareprimes") == 0) {
        execute_compareprimes(req, resp);
      } else {
        execute_work(req, resp);
      }
      worker_send_response(resp);
    }
    return NULL;
}
void* executeWork_disk(void* arg) {
    while(1) {
      Request_msg req = wstate.disk_work_queue.get_work();
      Response_msg resp(req.get_tag());
      // There is only one type here
      execute_work(req, resp);
      worker_send_response(resp);
    }
    return NULL;
}

void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.
  printf("**** Initializing worker: %s ****\n", params.get_arg("name").c_str());
  pthread_t thread_1;
  pthread_t thread_2;
  pthread_t thread_3;
  pthread_create(&thread_1, NULL, executeWork_cpu, NULL);
  pthread_create(&thread_2, NULL, executeWork_cpu, NULL);
  pthread_create(&thread_3, NULL, executeWork_disk, NULL);
}

void* executeWork(void* arg) {
  Request_msg* req = (Request_msg*) arg;
  Response_msg resp((*req).get_tag());
  if ((*req).get_arg("cmd").compare("compareprimes") == 0) {
    // The primerange command needs to be special cased since it is
    // built on 4 calls to execute_work.  All other requests
    // from the client are one-to-one with calls to
    // execute_work.
    execute_compareprimes(*req, resp);
  } else {
    // actually perform the work.  The response string is filled in by
    // 'execute_work'
    execute_work(*req, resp);
  }
  // send a response string to the master
  free(arg);
  worker_send_response(resp);
  return NULL;
}

void worker_handle_request(const Request_msg& req) {
   if(req.get_arg("cmd").compare("mostviewed") == 0) {
        wstate.disk_work_queue.put_work(req);
        return;
   }
    wstate.cpu_work_queue.put_work(req);
/*pthread_t thread_id;
  pthread_attr_t attr; // thread attribute
  Request_msg* cpyReq = new Request_msg(req);
  // set thread detachstate attribute to DETACHED 
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
  pthread_create(&thread_id, &attr, executeWork, cpyReq);*/
}
