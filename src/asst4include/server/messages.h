#ifndef __LIBASST4_MESSAGES_H__
#define __LIBASST4_MESSAGES_H__

#include <map>
#include <string>


class Request_msg {

  private:
     std::map<std::string, std::string> dict;
     std::string request_str;
     int tag;

  public:
  Request_msg(int tag);
  Request_msg(int tag, const std::string& str);
  Request_msg(int tag, const Request_msg& j);
  Request_msg(const Request_msg& j); // copy constructor

  std::string get_arg(const std::string& name) const;
  void set_arg(const std::string& key, const std::string& value);

  std::string get_request_string() const;
  int get_tag() const { return tag; }
};


class Response_msg {

private:
  int tag;
  std::string resp_str;

public:

  Response_msg(int arg_tag) {
    tag = arg_tag;
  }

  std::string get_response() const {
    return resp_str;
  }

  void set_response(const std::string& str) {
    resp_str = str;
  }

  int get_tag() const { return tag; }

};



#endif
