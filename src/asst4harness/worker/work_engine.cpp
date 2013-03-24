// Copyright 2013 Course Staff.

#include <boost/make_shared.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <fstream> // NOLINT
#include <map>

#include "server/messages.h"
#include "server/worker.h"

static char* balloon_allocation = NULL;

bool forceDiskReads;
std::string ioJobFilebase;
pthread_mutex_t ioJobCounterLock;
int lastIOJobFileIndex = 0;


struct Date {
  int month;
  int day;
  int year;

  Date() {
    year = month = day = 0;
  }

  Date(int argYear, int argMonth, int argDay) {
    set(argYear, argMonth, argDay);
  }

  void set(int argYear, int argMonth, int argDay) {
    year = argYear;
    month = argMonth;
    day = argDay;
  }

  void parse(const std::string& str) {
    sscanf(str.c_str(), "%04d-%02d-%02d", &year, &month, &day);  // NOLINT
  }

  bool within(const Date& start, const Date& end) {
    if ( year < start.year ||
         (year == start.year && month < start.month) ||
         (year == start.year && month == start.month && day < start.day))
      return false;

    if ( year > end.year ||
         (year == end.year && month > end.month) ||
         (year == end.year && month == end.month && day >= end.day))
      return false;

    return true;
  }

  std::string toString() const {
    char str[1024];
    snprintf(str, sizeof(str), "%04d-%02d-%02d", year, month, day);
    return std::string(str);
  }
};

std::string find_most_popular(const std::string& filename,
                              const Date& startDate, const Date& endDate) {
  std::ifstream db;

  db.open(filename.c_str());

  if (!db.is_open()) {
    DLOG(ERROR) << "Could not open pageviews file " << filename;
    return std::string("Could not open pageviews file");
  }

  std::string timestamp;
  std::string page_url;
  std::string browser;

  std::map<std::string, int> page_counts;

  Date viewDate;

  while (db.good()) {
    getline(db, timestamp);
    getline(db, page_url);
    getline(db, browser);

    // if it is not a lecture, ignore it
    if (page_url.find("lecture/") == std::string::npos)
      continue;

    viewDate.parse(timestamp);

    // if the item is in the date range, add to counts
    if (viewDate.within(startDate, endDate)) {
        if (page_counts.find(page_url) == page_counts.end()) {
            page_counts[page_url] = 1;
      } else {
            page_counts[page_url]++;
        }
    }
  }

  std::string mostViewed;
  int mostViewedCount = 0;

  std::map<std::string, int>::iterator it;
  for (it = page_counts.begin(); it != page_counts.end(); it++) {
    if (it->second > mostViewedCount) {
      mostViewedCount = it->second;
      mostViewed = it->first;
    }
  }

  char str[1024];
  snprintf(str, sizeof(str),
           "%s -- %d views", mostViewed.c_str(), mostViewedCount);
  return std::string(str);
}

void find_popular_pages(const Request_msg& req, Response_msg& resp) {

  Date startDate;
  Date endDate;

  startDate.parse(req.get_arg("start"));
  endDate.parse(req.get_arg("end"));

  int fileIndex = 0;
  if (forceDiskReads) {
    const int NUM_IO_JOB_FILES = 4;

    pthread_mutex_lock(&ioJobCounterLock);
    fileIndex = lastIOJobFileIndex++;
    if (lastIOJobFileIndex >= NUM_IO_JOB_FILES)
      lastIOJobFileIndex = 0;
    pthread_mutex_unlock(&ioJobCounterLock);
  }

  char tmp_buffer[2048];
  sprintf(tmp_buffer, "%s_%02d.txt", ioJobFilebase.c_str(), fileIndex);
  std::string result = find_most_popular(std::string(tmp_buffer), startDate, endDate);

  resp.set_response(result);
}


void high_compute_job(const Request_msg& req, Response_msg& resp) {

  const char* motivation[16] = {
    "You are going to do a great project",
    "OMG, 418 is so gr8!",
    "Come to lecture, there might be donuts!",
    "Write a great lecture comment on your favorite idea in the class",
    "Bring out all the stops in assignment 4.",
    "Ask questions. Ask questions. Ask questions",
    "Flatter your TAs with compliments",
    "Worse is better. Keep it simple...",
    "You will perform amazingly on exam 2",
    "You will PWN your classmates in the parallelism competition",
    "Exams are all just fun and games",
    "Do as best as you can and just have fun!",
    "Laugh at Kayvon's jokes",
    "Do a great project, and it all works out in the end",
    "Be careful not to optimize prematurely",
    "If all else fails... buy Kayvon donuts"
  };

  int iters = 175 * 1000 * 1000;
  unsigned int seed = atoi(req.get_arg("x").c_str());

  for (int i=0; i<iters; i++) {
    seed = rand_r(&seed);
  }

  int idx = seed % 16;
  resp.set_response(motivation[idx]);
}

void mini_compute_job(const Request_msg& req, Response_msg& resp) {
  char tmp_buffer[1024];
  int number = atoi(req.get_arg("x").c_str());
  int square = number * number;
  sprintf(tmp_buffer, "%d", square);
  resp.set_response(tmp_buffer);
}

void high_mem_job(const Request_msg& req, Response_msg& resp) {

  // this job will allocate 512 MB of RAM, and then write to it
  // several times
  int buffer_size = 512 * 1024 * 1024;
  char* allocation = new char[buffer_size];

  for (int i=0; i<buffer_size; i++)
    allocation[i] = 0;

  for (int iter=0; iter<2; iter++) {
    for (int i=0; i<buffer_size; i++) {
      allocation[i] += static_cast<char>(iter);
    }
  }

  resp.set_response("my result");
}

void count_primes_job(const Request_msg& req, Response_msg& resp) {

  int N = atoi(req.get_arg("n").c_str());

  int NUM_ITER = 10;
  int count;

  for (int iter = 0; iter < NUM_ITER; iter++) {
    count = (N >= 2) ? 1 : 0; // since 2 is prime

    for (int i = 3; i < N; i+=2) {    // For every odd number

      int prime;
      int div1, div2, rem;

      prime = i;

      // Keep searching for divisor until rem == 0 (i.e. non prime),
      // or we've reached the sqrt of prime (when div1 > div2)

      div1 = 1;
      do {
        div1 += 2;            // Divide by 3, 5, 7, ...
        div2 = prime / div1;  // Find the dividend
        rem = prime % div1;   // Find remainder
      } while (rem != 0 && div1 <= div2);

      if (rem != 0 || div1 == prime) {
        // prime is really a prime
        count++;
      }
    }
  }

  char tmp_buffer[32];
  sprintf(tmp_buffer, "%d", count);
  resp.set_response(tmp_buffer);
}



void execute_work(const Request_msg& req, Response_msg& resp) {

  std::string cmd = req.get_arg("cmd");

  if (cmd.compare("mostviewed") == 0) {
    find_popular_pages(req, resp);
  }
  else if (cmd.compare("418wisdom") == 0) {
    high_compute_job(req, resp);
  }
  else if (cmd.compare("countprimes") == 0) {
    count_primes_job(req, resp);
  }
  else if (cmd.compare("minicompute") == 0) {
    mini_compute_job(req, resp);
  }
  else if (cmd.compare("highmem") == 0) {
    high_mem_job(req, resp);
  }
  else {
    resp.set_response("unknown command");
  }
}


void init_work_engine(bool forceDiskIO, const std::string& assetsDir) {

  char tmp_buffer[2048];

  forceDiskReads = forceDiskIO;

  sprintf(tmp_buffer, "%s/pageviews_med", assetsDir.c_str());
  ioJobFilebase = std::string(tmp_buffer);

  if (forceDiskReads) {

    // we'll using this mutex to protect our counter of what file to
    // read next
    pthread_mutex_init(&ioJobCounterLock, NULL);

    // Grab 1 GB of memory in an attempt to prevent buffer cache
    // effects from accelerating IO.  I'm starting to think this is
    // hopeless on AWS machines, because who knows how the disk is
    // virtualized.

    int num_bytes = 1024 * 1024 * 1024;
    balloon_allocation = new char[num_bytes];
    for (int i=0; i<num_bytes; i++)
      balloon_allocation[i] = 0;
  }
}
