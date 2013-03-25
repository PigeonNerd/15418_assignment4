// Copyright 2013 15418 Course Staff.

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>

#include "server/messages.h"
#include "server/worker.h"


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


void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  printf("**** Initializing worker: %s ****\n", params.get_arg("name").c_str());

}

void* executeWork(const Request_msg& req) {

  Response_msg resp(req.get_tag());

  if (req.get_arg("cmd").compare("compareprimes") == 0) {

    // The primerange command needs to be special cased since it is
    // built on 4 calls to execute_execute work.  All other requests
    // from the client are one-to-one with calls to
    // execute_work.

    execute_compareprimes(req, resp);

  } else {

    // actually perform the work.  The response string is filled in by
    // 'execute_work'
    execute_work(req, resp);

  }

  // send a response string to the master
  worker_send_response(resp);

}

void worker_handle_request(const Request_msg& req) {
  pthread_t thread_id;
  pthread_create(&thread_id, NULL, executeWork, &req);
}
