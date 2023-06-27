// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_sal.h"
#include "rgw_tools.h"
#include "rgw_zone.h"
#include "rgw_notify_event_type.h"
#include <boost/container/flat_map.hpp>

class XMLObj;

struct rgw_s3_key_filter {
  std::string prefix_rule;
  std::string suffix_rule;
  std::string regex_rule;

  bool has_content() const;

  void dump(Formatter *f) const;
  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(prefix_rule, bl);
    encode(suffix_rule, bl);
    encode(regex_rule, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(prefix_rule, bl);
    decode(suffix_rule, bl);
    decode(regex_rule, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_s3_key_filter)

using KeyValueMap = boost::container::flat_map<std::string, std::string>;
using KeyMultiValueMap = std::multimap<std::string, std::string>;

struct rgw_s3_key_value_filter {
  KeyValueMap kv;
  
  bool has_content() const;

  void dump(Formatter *f) const;
  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(kv, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(kv, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_s3_key_value_filter)

struct rgw_s3_filter {
  rgw_s3_key_filter key_filter;
  rgw_s3_key_value_filter metadata_filter;
  rgw_s3_key_value_filter tag_filter;

  bool has_content() const;

  void dump(Formatter *f) const;
  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
  
  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(key_filter, bl);
    encode(metadata_filter, bl);
    encode(tag_filter, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(key_filter, bl);
    decode(metadata_filter, bl);
    if (struct_v >= 2) {
        decode(tag_filter, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(rgw_s3_filter)

using OptionalFilter = std::optional<rgw_s3_filter>;

struct rgw_pubsub_topic_filter;
/* S3 notification configuration
 * based on: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTnotification.html
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <TopicConfiguration>
    <Filter>
      <S3Key>
        <FilterRule>
          <Name>suffix</Name>
          <Value>jpg</Value>
        </FilterRule>
      </S3Key>
      <S3Metadata>
        <FilterRule>
          <Name></Name>
          <Value></Value>
        </FilterRule>
      </S3Metadata>
      <S3Tags>
        <FilterRule>
          <Name></Name>
          <Value></Value>
        </FilterRule>
      </S3Tags>
    </Filter>
    <Id>notification1</Id>
    <Topic>arn:aws:sns:<region>:<account>:<topic></Topic>
    <Event>s3:ObjectCreated:*</Event>
    <Event>s3:ObjectRemoved:*</Event>
  </TopicConfiguration>
</NotificationConfiguration>
*/
struct rgw_pubsub_s3_notification {
  // notification id
  std::string id;
  // types of events
  rgw::notify::EventTypeList events;
  // topic ARN
  std::string topic_arn;
  // filter rules
  rgw_s3_filter filter;

  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;

  rgw_pubsub_s3_notification() = default;
  // construct from rgw_pubsub_topic_filter (used by get/list notifications)
  explicit rgw_pubsub_s3_notification(const rgw_pubsub_topic_filter& topic_filter);
};

// return true if the key matches the prefix/suffix/regex rules of the key filter
bool match(const rgw_s3_key_filter& filter, const std::string& key);

// return true if the key matches the metadata rules of the metadata filter
bool match(const rgw_s3_key_value_filter& filter, const KeyValueMap& kv);

// return true if the key matches the tag rules of the tag filter
bool match(const rgw_s3_key_value_filter& filter, const KeyMultiValueMap& kv);

// return true if the event type matches (equal or contained in) one of the events in the list
bool match(const rgw::notify::EventTypeList& events, rgw::notify::EventType event);

struct rgw_pubsub_s3_notifications {
  std::list<rgw_pubsub_s3_notification> list;
  bool decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;
};

/* S3 event records structure
 * based on: https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
{  
"Records":[  
  {
    "eventVersion":""
    "eventSource":"",
    "awsRegion":"",
    "eventTime":"",
    "eventName":"",
    "userIdentity":{  
      "principalId":""
    },
    "requestParameters":{
      "sourceIPAddress":""
    },
    "responseElements":{
      "x-amz-request-id":"",
      "x-amz-id-2":""
    },
    "s3":{
      "s3SchemaVersion":"1.0",
      "configurationId":"",
      "bucket":{
        "name":"",
        "ownerIdentity":{
          "principalId":""
        },
        "arn":""
        "id": ""
      },
      "object":{
        "key":"",
        "size": ,
        "eTag":"",
        "versionId":"",
        "sequencer": "",
        "metadata": ""
        "tags": ""
      }
    },
    "eventId":"",
  }
]
}*/

struct rgw_pubsub_s3_event {
  constexpr static const char* const json_type_plural = "Records";
  std::string eventVersion = "2.2";
  // aws:s3
  std::string eventSource = "ceph:s3";
  // zonegroup
  std::string awsRegion;
  // time of the request
  ceph::real_time eventTime;
  // type of the event
  std::string eventName;
  // user that sent the request
  std::string userIdentity;
  // IP address of source of the request (not implemented)
  std::string sourceIPAddress;
  // request ID (not implemented)
  std::string x_amz_request_id;
  // radosgw that received the request
  std::string x_amz_id_2;
  std::string s3SchemaVersion = "1.0";
  // ID received in the notification request
  std::string configurationId;
  // bucket name
  std::string bucket_name;
  // bucket owner
  std::string bucket_ownerIdentity;
  // bucket ARN
  std::string bucket_arn;
  // object key
  std::string object_key;
  // object size
  uint64_t object_size = 0;
  // object etag
  std::string object_etag;
  // object version id bucket is versioned
  std::string object_versionId;
  // hexadecimal value used to determine event order for specific key
  std::string object_sequencer;
  // this is an rgw extension (not S3 standard)
  // used to store a globally unique identifier of the event
  // that could be used for acking or any other identification of the event
  std::string id;
  // this is an rgw extension holding the internal bucket id
  std::string bucket_id;
  // meta data
  KeyValueMap x_meta_map;
  // tags
  KeyMultiValueMap tags;
  // opaque data received from the topic
  // could be used to identify the gateway
  std::string opaque_data;

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 1, bl);
    encode(eventVersion, bl);
    encode(eventSource, bl);
    encode(awsRegion, bl);
    encode(eventTime, bl);
    encode(eventName, bl);
    encode(userIdentity, bl);
    encode(sourceIPAddress, bl);
    encode(x_amz_request_id, bl);
    encode(x_amz_id_2, bl);
    encode(s3SchemaVersion, bl);
    encode(configurationId, bl);
    encode(bucket_name, bl);
    encode(bucket_ownerIdentity, bl);
    encode(bucket_arn, bl);
    encode(object_key, bl);
    encode(object_size, bl);
    encode(object_etag, bl);
    encode(object_versionId, bl);
    encode(object_sequencer, bl);
    encode(id, bl);
    encode(bucket_id, bl);
    encode(x_meta_map, bl);
    encode(tags, bl);
    encode(opaque_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(4, bl);
    decode(eventVersion, bl);
    decode(eventSource, bl);
    decode(awsRegion, bl);
    decode(eventTime, bl);
    decode(eventName, bl);
    decode(userIdentity, bl);
    decode(sourceIPAddress, bl);
    decode(x_amz_request_id, bl);
    decode(x_amz_id_2, bl);
    decode(s3SchemaVersion, bl);
    decode(configurationId, bl);
    decode(bucket_name, bl);
    decode(bucket_ownerIdentity, bl);
    decode(bucket_arn, bl);
    decode(object_key, bl);
    decode(object_size, bl);
    decode(object_etag, bl);
    decode(object_versionId, bl);
    decode(object_sequencer, bl);
    decode(id, bl);
    if (struct_v >= 2) {
      decode(bucket_id, bl);
      decode(x_meta_map, bl);
    }
    if (struct_v >= 3) {
      decode(tags, bl);
    }
    if (struct_v >= 4) {
      decode(opaque_data, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_s3_event)

// setting a unique ID for an event based on object hash and timestamp
void set_event_id(std::string& id, const std::string& hash, const utime_t& ts);

struct rgw_pubsub_dest {
  std::string push_endpoint;
  std::string push_endpoint_args;
  std::string arn_topic;
  bool stored_secret = false;
  bool persistent = false;

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 1, bl);
    encode("", bl);
    encode("", bl);
    encode(push_endpoint, bl);
    encode(push_endpoint_args, bl);
    encode(arn_topic, bl);
    encode(stored_secret, bl);
    encode(persistent, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(5, bl);
    std::string dummy;
    decode(dummy, bl);
    decode(dummy, bl);
    decode(push_endpoint, bl);
    if (struct_v >= 2) {
        decode(push_endpoint_args, bl);
    }
    if (struct_v >= 3) {
        decode(arn_topic, bl);
    }
    if (struct_v >= 4) {
        decode(stored_secret, bl);
    }
    if (struct_v >= 5) {
        decode(persistent, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;
  std::string to_json_str() const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_dest)

struct rgw_pubsub_topic {
  rgw_user user;
  std::string name;
  rgw_pubsub_dest dest;
  std::string arn;
  std::string opaque_data;

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    encode(user, bl);
    encode(name, bl);
    encode(dest, bl);
    encode(arn, bl);
    encode(opaque_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(user, bl);
    decode(name, bl);
    if (struct_v >= 2) {
      decode(dest, bl);
      decode(arn, bl);
    }
    if (struct_v >= 3) {
      decode(opaque_data, bl);
    }
    DECODE_FINISH(bl);
  }

  std::string to_str() const {
    return user.tenant + "/" + name;
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;
  void dump_xml_as_attributes(Formatter *f) const;

  bool operator<(const rgw_pubsub_topic& t) const {
    return to_str().compare(t.to_str());
  }
};
WRITE_CLASS_ENCODER(rgw_pubsub_topic)

// this struct deprecated and remain only for backward compatibility
struct rgw_pubsub_topic_subs {
  rgw_pubsub_topic topic;
  std::set<std::string> subs;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(topic, bl);
    encode(subs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(topic, bl);
    decode(subs, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_topic_subs)

struct rgw_pubsub_topic_filter {
  rgw_pubsub_topic topic;
  rgw::notify::EventTypeList events;
  std::string s3_id;
  rgw_s3_filter s3_filter;

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    encode(topic, bl);
    // events are stored as a vector of std::strings
    std::vector<std::string> tmp_events;
    std::transform(events.begin(), events.end(), std::back_inserter(tmp_events), rgw::notify::to_string);
    encode(tmp_events, bl);
    encode(s3_id, bl);
    encode(s3_filter, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(topic, bl);
    // events are stored as a vector of std::strings
    events.clear();
    std::vector<std::string> tmp_events;
    decode(tmp_events, bl);
    std::transform(tmp_events.begin(), tmp_events.end(), std::back_inserter(events), rgw::notify::from_string);
    if (struct_v >= 2) {
      decode(s3_id, bl);
    }
    if (struct_v >= 3) {
      decode(s3_filter, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_topic_filter)

struct rgw_pubsub_bucket_topics {
  std::map<std::string, rgw_pubsub_topic_filter> topics;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(topics, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(topics, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_bucket_topics)

struct rgw_pubsub_topics {
  std::map<std::string, rgw_pubsub_topic> topics;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(topics, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    if (struct_v >= 2) {
      decode(topics, bl);
    } else {
      std::map<std::string, rgw_pubsub_topic_subs> v1topics;
      decode(v1topics, bl);
      std::transform(v1topics.begin(), v1topics.end(), std::inserter(topics, topics.end()),
          [](const auto& entry) {
            return std::pair<std::string, rgw_pubsub_topic>(entry.first, entry.second.topic); 
          });
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;
};
WRITE_CLASS_ENCODER(rgw_pubsub_topics)

class RGWPubSub
{
  friend class Bucket;

  rgw::sal::Driver* const driver;
  const std::string tenant;

  int read_topics(const DoutPrefixProvider *dpp, rgw_pubsub_topics& result, 
      RGWObjVersionTracker* objv_tracker, optional_yield y) const;
  int write_topics(const DoutPrefixProvider *dpp, const rgw_pubsub_topics& topics,
			RGWObjVersionTracker* objv_tracker, optional_yield y) const;

public:
  RGWPubSub(rgw::sal::Driver* _driver, const std::string& tenant);

  class Bucket {
    friend class RGWPubSub;
    const RGWPubSub& ps;
    rgw::sal::Bucket* const bucket;

    // read the list of topics associated with a bucket and populate into result
    // use version tacker to enforce atomicity between read/write
    // return 0 on success or if no topic was associated with the bucket, error code otherwise
    int read_topics(const DoutPrefixProvider *dpp, rgw_pubsub_bucket_topics& result, 
        RGWObjVersionTracker* objv_tracker, optional_yield y) const;
    // set the list of topics associated with a bucket
    // use version tacker to enforce atomicity between read/write
    // return 0 on success, error code otherwise
    int write_topics(const DoutPrefixProvider *dpp, const rgw_pubsub_bucket_topics& topics,
		     RGWObjVersionTracker* objv_tracker, optional_yield y) const;
    int remove_notification_inner(const DoutPrefixProvider *dpp, const std::string &topic_name,
                                  optional_yield y) const;
  public:
    Bucket(const RGWPubSub& _ps, rgw::sal::Bucket* _bucket) : 
      ps(_ps), bucket(_bucket)
    {}

    // get the list of topics associated with a bucket and populate into result
    // return 0 on success or if no topic was associated with the bucket, error code otherwise
    int get_topics(const DoutPrefixProvider *dpp, rgw_pubsub_bucket_topics& result, optional_yield y) const {
      return read_topics(dpp, result, nullptr, y);
    }
    // get a bucket_topic with by its name and populate it into "result"
    // return -ENOENT if the topic does not exists
    // return 0 on success, error code otherwise
    int get_notification_by_id(const DoutPrefixProvider *dpp, const std::string &notification_id,
                               const std::string &topic_name, rgw_pubsub_topic_filter &result,
                               optional_yield y) const;
    // adds a topic + filter (event list, and possibly name metadata or tags filters) to a bucket
    // assigning a notification name is optional (needed for S3 compatible notifications)
    // if the topic already exist on the bucket, the filter event list may be updated
    // for S3 compliant notifications the version with: s3_filter and notif_name should be used
    // return -ENOENT if the topic does not exists
    // return 0 on success, error code otherwise
    int create_notification(const DoutPrefixProvider *dpp, const std::string& topic_name, 
        const rgw::notify::EventTypeList& events, optional_yield y) const;
    int create_notification(const DoutPrefixProvider *dpp, const std::string& topic_name, 
        const rgw::notify::EventTypeList& events, OptionalFilter s3_filter, const std::string& notif_name, optional_yield y) const;
    // remove a topic and filter from bucket
    // if the topic does not exists on the bucket it is a no-op (considered success)
    // return -ENOENT if the notification-id/topic does not exists
    // return 0 on success, error code otherwise
    int remove_notification_by_id_and_topic_name(const DoutPrefixProvider *dpp, const std::string &notif_id,
                                                 const std::string &topic_name, optional_yield y) const;
    int remove_notification(const DoutPrefixProvider *dpp, const std::string& topic_name, optional_yield y) const;
    // remove all notifications (and autogenerated topics) associated with the bucket
    // return 0 on success or if no topic was associated with the bucket, error code otherwise
    int remove_notifications(const DoutPrefixProvider *dpp, optional_yield y) const;
  };

  // get the list of topics
  // return 0 on success or if no topic was associated with the bucket, error code otherwise
  int get_topics(const DoutPrefixProvider *dpp, rgw_pubsub_topics& result, optional_yield y) const {
    return read_topics(dpp, result, nullptr, y);
  }
  // get a topic with by its name and populate it into "result"
  // return -ENOENT if the topic does not exists 
  // return 0 on success, error code otherwise
  int get_topic(const DoutPrefixProvider *dpp, const std::string& name, rgw_pubsub_topic& result, optional_yield y) const;
  // create a topic with a name only
  // if the topic already exists it is a no-op (considered success)
  // return 0 on success, error code otherwise
  int create_topic(const DoutPrefixProvider *dpp, const std::string& name, optional_yield y) const;
  // create a topic with push destination information and ARN
  // if the topic already exists the destination and ARN values may be updated (considered succsess)
  // return 0 on success, error code otherwise
  int create_topic(const DoutPrefixProvider *dpp, const std::string& name, const rgw_pubsub_dest& dest, 
      const std::string& arn, const std::string& opaque_data, optional_yield y) const;
  // remove a topic according to its name
  // if the topic does not exists it is a no-op (considered success)
  // return 0 on success, error code otherwise
  int remove_topic(const DoutPrefixProvider *dpp, const std::string& name, optional_yield y) const;
};

