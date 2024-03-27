// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "topic_migration.h"
#include "services/svc_zone.h"
#include "rgw_sal_rados.h"

namespace rgwrados::topic_migration {

namespace {

int deconstruct_topics_oid(const std::string& bucket_topics_oid, std::string& tenant, std::string& bucket_name,
                           std::string& marker, const DoutPrefixProvider* dpp) {
  auto pos = bucket_topics_oid.find(rgw::sal::pubsub_bucket_oid_infix);
  if (pos == std::string::npos) {
    ldpp_dout(dpp, 1) << "ERROR: bucket_topics_oid:" << bucket_topics_oid << " doesn't contain " << rgw::sal::pubsub_bucket_oid_infix
                      << " after tenant name!" << dendl;
    return -EINVAL;
  }
  const size_t prefix_len = rgw::sal::pubsub_oid_prefix.size();
  tenant = bucket_topics_oid.substr(prefix_len, pos - prefix_len);

  auto bucket_name_marker = bucket_topics_oid.substr(pos + rgw::sal::pubsub_bucket_oid_infix.size());
  pos = bucket_name_marker.find('/');
  if (pos == std::string::npos) {
    ldpp_dout(dpp, 1) << "ERROR: bucket_topics_oid:" << bucket_topics_oid << " doesn't contain / after bucket name!" << dendl;
    return -EINVAL;
  }
  bucket_name = bucket_name_marker.substr(0, pos);
  marker = bucket_name_marker.substr(pos + 1);

  return 0;
}

// migrate v1 notification metadata for a single bucket
int migrate_notification(const DoutPrefixProvider* dpp, optional_yield y,
                         rgw::sal::RadosStore* driver, const rgw_raw_obj& obj)
{
  // parse bucket name and marker of out "pubsub.{tenant}.bucket.{name}/{marker}"
  auto* rados = driver->getRados()->get_rados_handle();
  std::string tenant;
  std::string bucket_name;
  std::string marker;
  int r = deconstruct_topics_oid(obj.oid, tenant, bucket_name, marker, dpp);
  if (r < 0) {
    std::string s = "failed to read tenant, bucket name and marker from " + obj.to_str()
                  + " with: " + cpp_strerror(r)
                  + " expected format pubsub.{tenant}.bucket.{name}/{marker}!";
    rgw_clog_warn(rados, s);
    return r;
  }

  // migrate the notifications
  rgw_pubsub_bucket_topics bucket_topics;
  rgw_bucket rgw_bucket_info(tenant, bucket_name);
  rgw_bucket_info.marker = marker;
  rgw::sal::RadosBucket rados_bucket(driver, rgw_bucket_info);
  RGWObjVersionTracker bucket_topics_objv;
  r = rados_bucket.read_topics(bucket_topics, &bucket_topics_objv, y, dpp);
  if (r == -ENOENT) {
    return 0; // ok, someone else already migrated
  }
  if (r < 0) {
    std::string s = "failed to read v1 bucket notifications from " + obj.to_str()
                  + " with: " + cpp_strerror(r);
    rgw_clog_warn(rados, s);
    return r;
  }

  // in a for-loop that retries ECANCELED errors:
  // {
  // load the corresponding bucket by name
  // break if marker doesn't match loaded bucket's
  // break if RGW_ATTR_BUCKET_NOTIFICATION xattr already exists
  // write RGW_ATTR_BUCKET_NOTIFICATION xattr
  // }
  std::unique_ptr<rgw::sal::Bucket> bucket;
  r = -ECANCELED;
  for (auto i = 0u; i < 15u && r == -ECANCELED; ++i) {
    r = driver->load_bucket(dpp, rgw_bucket_info, &bucket, y);
    if (r == -ENOENT) {
      break; // bucket is deleted, we should delete the v1 notification
    }
    if (r < 0) {
      std::string s = "failed to load the bucket from " + obj.to_str()
                    + " with: " + cpp_strerror(r);
      rgw_clog_warn(rados, s);
      return r;
    }

    if (bucket->get_marker() != marker) {
      break;
    }

    rgw_pubsub_bucket_topics v2_bucket_topics;
    if(bucket->get_attrs().contains(RGW_ATTR_BUCKET_NOTIFICATION)) {
      r = get_bucket_notifications(dpp, bucket.get(), v2_bucket_topics);
      if (r < 0) {
        ldpp_dout(dpp, 1) << "ERROR: failed to decode bucket topics for bucket: "
                          << bucket->get_name() << ", discarding the current v2 bucket topics" << dendl;
      }
    }

    // merge v1 and v2 bucket topics
    for (auto& [topic_name, bucket_topic]: bucket_topics.topics) {
      bucket_topic.topic.name = bucket_topic.topic.dest.arn_topic;
      v2_bucket_topics.topics.insert({topic_name, bucket_topic});
    }

    bufferlist bl;
    v2_bucket_topics.encode(bl);
    bucket->get_attrs()[RGW_ATTR_BUCKET_NOTIFICATION] = std::move(bl);
    r = bucket->put_info(dpp, false, real_time(), y);
    if (r != -ECANCELED && r < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << dendl;
      std::string s = "ERROR: failed writing bucket instance info: " + cpp_strerror(-r);
      rgw_clog_warn(rados, s);
      return r;
    }
  }

  // delete v1 notification obj with Bucket::remove_topics()
  r = rados_bucket.remove_topics(&bucket_topics_objv, y, dpp);
  if (r == -ECANCELED) {
    return 0; // ok, someone else already migrated
  }
  if (r < 0) {
    std::string s = "failed to remove migrated v1 bucket topics obj" + obj.to_str() + " with: " + cpp_strerror(-r);
    rgw_clog_warn(rados, s);
    return r;
  }

  return r;
}

// migrate topics for a given tenant
int migrate_topics(const DoutPrefixProvider* dpp, optional_yield y,
                   rgw::sal::RadosStore* driver,
                   const rgw_raw_obj& topics_obj)
{
  // parse tenant name out of topics_obj "pubsub.{tenant}"
  auto* rados = driver->getRados()->get_rados_handle();
  std::string tenant;
  const auto& topics_obj_oid = topics_obj.oid;
  if (auto pos = topics_obj_oid.find(rgw::sal::pubsub_oid_prefix); pos != std::string::npos) {
    tenant = topics_obj_oid.substr(std::string(rgw::sal::pubsub_oid_prefix).size());
  } else {
    std::string s = "failed to read tenant from name from oid: " + topics_obj_oid + " with: " + cpp_strerror(-EINVAL);
    rgw_clog_warn(rados, s);
    return -EINVAL;
  }

  // migrate the topics
  rgw_pubsub_topics topics;
  RGWObjVersionTracker topics_objv;
  int r = driver->read_topics(tenant, topics, &topics_objv, y, dpp);
  if (r == -ENOENT) {
    return 0; // ok, someone else already migrated
  }
  if (r < 0) {
    std::string s = "failed to read v1 topics from " + topics_obj.to_str() + " error: " + cpp_strerror(-r);
    rgw_clog_warn(rados, s);
    return r;
  }

  constexpr bool exclusive = true; // don't overwrite any existing v2 metadata
  for (const auto& [name, topic] : topics.topics) {
    if (topic.name != topic.dest.arn_topic) {
      continue;
    }
    // write the v2 topic
    RGWObjVersionTracker objv;
    objv.generate_new_write_ver(dpp->get_cct());
    r = driver->write_topic_v2(topic, exclusive, objv, y, dpp);
    if (r == -EEXIST) {
      continue; // ok, someone else already migrated
    }
    if (r < 0) {
      std::string s = "v1 topic migration for " + topic.name + " failed with: " + cpp_strerror(r);
      rgw_clog_warn(rados, s);
      return r;
    }
  }

  // remove the v1 topics metadata (this destroys the lock too)
  r = driver->remove_topics(tenant, &topics_objv, y, dpp);
  if (r == -ECANCELED) {
    return 0; // ok, someone else already migrated
  }
  if (r < 0) {
    std::string s = "failed to remove migrated v1 topics obj "
                  + topics_obj.to_str() + " with: " + cpp_strerror(r);
    rgw_clog_warn(rados, s);
    return r;
  }
  return r;
}

} // anonymous namespace

int migrate(const DoutPrefixProvider* dpp,
            rgw::sal::RadosStore* driver,
            boost::asio::io_context& context,
            spawn::yield_context yield)
{
  auto y = optional_yield{context, yield};

  ldpp_dout(dpp, 1) << "starting v1 topic migration.." << dendl;

  librados::Rados* rados = driver->getRados()->get_rados_handle();
  const rgw_pool& pool = driver->svc()->zone->get_zone_params().log_pool;
  librados::IoCtx ioctx;
  int r = rgw_init_ioctx(dpp, rados, pool, ioctx);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "failed to initialize log pool for listing with: "
        << cpp_strerror(r) << dendl;
    return r;
  }

  // loop over all objects with oid prefix "pubsub."
  auto filter = rgw::AccessListFilterPrefix(rgw::sal::pubsub_oid_prefix);
  constexpr uint32_t max = 100;
  std::string marker;
  bool truncated = false;

  std::vector<std::string> oids;
  std::vector<std::string> topics_oid;
  do {
    oids.clear();
    r = rgw_list_pool(dpp, ioctx, max, filter, marker, &oids, &truncated);
    if (r == -ENOENT) {
      r = 0;
      break;
    }
    if (r < 0) {
      ldpp_dout(dpp, 1) << "failed to list v1 topic metadata with: "
          << cpp_strerror(r) << dendl;
      return r;
    }

    std::string msg;
    for (const std::string& oid : oids) {
      if (oid.find(rgw::sal::pubsub_bucket_oid_infix) != oid.npos) {
        const auto obj = rgw_raw_obj{pool, oid};
        ldpp_dout(dpp, 4) << "migrating v1 bucket notifications " << oid << dendl;
        r = migrate_notification(dpp, y, driver, obj);
        ldpp_dout(dpp, 4) << "migrating v1 bucket notifications " << oid << " completed with: "
                          << ((r == 0)? "successful": cpp_strerror(r)) << dendl;
      } else {
        // topics will be migrated after we complete migrating the notifications
        topics_oid.push_back(oid);
      }
    }
    if (!oids.empty()) {
      marker = oids.back(); // update marker for next listing
    }
  } while (truncated);


  for (const std::string& oid : topics_oid) {
    const auto obj = rgw_raw_obj{pool, oid};
    ldpp_dout(dpp, 4) << "migrating v1 topics " << oid << dendl;
    r = migrate_topics(dpp, y, driver, obj);
    ldpp_dout(dpp, 4) << "migrating v1 topics " << oid << " completed with: "
                      << ((r == 0) ? "successful" : cpp_strerror(r)) << dendl;
  }

  ldpp_dout(dpp, 1) << "finished v1 topic migration" << dendl;
  return 0;
}

} // rgwrados::topic_migration
