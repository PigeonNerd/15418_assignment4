// Copyright 2013 Harry Q. Bovik (hbovik)
#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <vector>

#include "server/messages.h"
#include "server/master.h"
#include "tools/work_queue.h"

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  int num_pending_client_requests;
  int num_waiting;
  //std::vector<Worker_handle>workersQueue;

  WorkQueue<Worker_handle>workersQueue;
  std::vector<Request_msg>waitingRequests;

  std:: map<int, Client_handle> requestsMap;

  Worker_handle my_worker;
  Client_handle waiting_client;

} mstate;

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;
  printf("The maximum number of workers %d\n", max_workers);
  // HOW TO SET THIS NUMBER ?
  //mstate.max_num_workers = max_workers;
  mstate.max_num_workers = 3;

  mstate.num_pending_client_requests = 0;
  mstate.num_waiting = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // here we temporally request 3 worker nodes
  for(int i = 0 ; i < 3 ; i++) {
      int tag = random();
      Request_msg req(tag);
      char name[20];
      sprintf(name, "my worker %d", i);
      req.set_arg("name", name);
      request_new_worker_node(req);
  }
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  mstate.workersQueue.put_work( worker_handle );

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.
  std::map<int, Client_handle>::iterator it = mstate.requestsMap.find(resp.get_tag());
  send_client_response(it->second, resp);
  mstate.requestsMap.erase(it);
  mstate.num_pending_client_requests--;

  // here means we do not have more work right now
  if( mstate.num_waiting == 0) {
    mstate.workersQueue.put_work( worker_handle );
  }else {
    mstate.num_pending_client_requests++;
    Request_msg thisRequest = mstate.waitingRequests.front();
    send_request_to_worker( worker_handle, thisRequest);
    mstate.waitingRequests.erase(mstate.waitingRequests.begin());
    mstate.num_waiting--;
  }
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {


  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  int tag = random();
  Request_msg worker_req(tag, client_req);

  // store the waiting client into the map
  mstate.requestsMap[tag] = client_handle;

  // we run out of workers
  if( mstate.num_pending_client_requests == mstate.max_num_workers) {
    mstate.waitingRequests.push_back(worker_req);
    mstate.num_waiting ++;
    //printf("number waiting : %d\n", mstate.num_waiting);
    return;
  }
  //printf("pending: %d\n", mstate.num_pending_client_requests);
  mstate.num_pending_client_requests++;
  // Fire off request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.

  Worker_handle thisWorker = mstate.workersQueue.get_work();
  send_request_to_worker(thisWorker, worker_req);
  //mstate.workersQueue.erase(mstate.workersQueue.begin());

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.
}


void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

}

