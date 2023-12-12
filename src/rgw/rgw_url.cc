// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <regex>
#include "include/cephfs/libcephfs.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw {

namespace {
  const auto USER_GROUP_IDX = 3;
  const auto PASSWORD_GROUP_IDX = 4;
  const auto HOST_GROUP_IDX = 5;

  const std::string schema_re = "([[:alpha:]]+:\\/\\/)";
  const std::string user_pass_re = "(([^:\\s]+):([^@\\s]+)@)?";
  const std::string host_port_re = "([[:alnum:].:-]+)";
  const std::string path_re = "(/[[:print:]]*)?";
}

bool parse_url_authority(const std::string &url, std::string &host, std::string &user, std::string &password,
                         CephContext *cct) {
  const std::string re = schema_re + user_pass_re + host_port_re + path_re;
  const std::regex url_regex(re, std::regex::icase);
  std::smatch url_match_result;

  if (std::regex_match(url, url_match_result, url_regex)) {
    if (cct) {
      ldout(cct, 1) << "Ali debugging in parse_url | regex matched, url: " << url << dendl;
      auto i = 0U;
      for (auto it: url_match_result) {
        ldout(cct, 1) << "Ali debugging the " << i++ << "th regex " << it.str() << dendl;
      }
    }
    auto size = url_match_result.size();
    std::string empty_str = "";
    host = (size > HOST_GROUP_IDX? url_match_result[HOST_GROUP_IDX]: empty_str);
    user = (size > USER_GROUP_IDX? url_match_result[USER_GROUP_IDX]: empty_str);
    password = (size > PASSWORD_GROUP_IDX? url_match_result[PASSWORD_GROUP_IDX]: empty_str);
    return true;
  }

  if (cct) {
    ldout(cct, 1) << "Ali debugging in parse_url | regex didn't match" << dendl;
  }
  return false;
}

bool parse_url_userinfo(const std::string& url, std::string& user, std::string& password) {
  const std::string re = schema_re + user_pass_re + host_port_re + path_re;
  const std::regex url_regex(re);
  std::smatch url_match_result;

  if (std::regex_match(url, url_match_result, url_regex)) {
    user = url_match_result[USER_GROUP_IDX];
    password = url_match_result[PASSWORD_GROUP_IDX];
    return true;
  }

  return false;
}
}

