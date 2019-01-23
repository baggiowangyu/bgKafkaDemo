// kafka_consumer.cpp : 定义控制台应用程序的入口点。
//

#include "stdafx.h"
#include "rdkafkacpp.h"
#include <chrono>
#include <time.h>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <algorithm>
#include <iterator>


void consume_cb(RdKafka::Message &message, void *opaque)
{
	switch (message.err()) {
	case RdKafka::ERR__TIMED_OUT:
		std::cout << "RdKafka::ERR__TIMED_OUT" << std::endl;
		break;
	case RdKafka::ERR_NO_ERROR:
		/* Real message */

		RdKafka::MessageTimestamp ts;
		ts = message.timestamp();
		if (ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
			std::string timeprefix;
			if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME) {
				timeprefix = "created time";
			}
			else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME) {
				timeprefix = "log append time";
			}


			unsigned long long milli = ts.timestamp + (unsigned long long)8 * 60 * 60 * 1000;//此处转化为东八区北京时间，如果是其它时区需要按需求修改
			auto mTime = std::chrono::milliseconds(milli);
			auto tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>(mTime);
			auto tt = std::chrono::system_clock::to_time_t(tp);
			tm timeinfo;
			::gmtime_s(&timeinfo, &tt);

			char s[60]{ 0 };
			::sprintf(s, "%04d-%02d-%02d %02d:%02d:%02d", timeinfo.tm_year + 1900, timeinfo.tm_mon + 1, timeinfo.tm_mday, timeinfo.tm_hour, timeinfo.tm_min, timeinfo.tm_sec);
#if 0
			std::stringstream ss;
			std::string dateStr;

			ss << timeinfo.tm_year + 1900 << "-"
				<< timeinfo.tm_mon + 1 << "-"
				<< timeinfo.tm_mday;
			ss >> dateStr;

			ss.clear();
			ss << timeinfo.tm_hour << ":"
				<< timeinfo.tm_min << ":"
				<< timeinfo.tm_sec;
			std::string timeStr;
			ss >> timeStr;

			std::string dateTimeStr;
			dateTimeStr += dateStr;
			dateTimeStr.push_back(' ');
			dateTimeStr += timeStr;
#endif // 0

			std::cout << "TimeStamp" << timeprefix << " " << s << std::endl;
		}

		std::cout << message.topic_name() << " offset" << message.offset() << "  partion " << message.partition() << " message: " << reinterpret_cast<char*>(message.payload()) << std::endl;
		break;

	case RdKafka::ERR__PARTITION_EOF:
		/* Last message */
		std::cout << "EOF reached for" << std::endl;
		break;

	case RdKafka::ERR__UNKNOWN_TOPIC:
	case RdKafka::ERR__UNKNOWN_PARTITION:
		std::cout << "Consume failed: " << message.errstr();
		break;

	default:
		/* Errors */
		std::cout << "Consume failed: " << message.errstr();
		break;
	}
}


int _tmain(int argc, _TCHAR* argv[])
{
	//std::string brokers = "192.168.0.127:9092,192.168.0.128:9092,192.168.0.129:9092";
	std::string brokers = "127.0.0.1:9092";
	std::string errstr;
	std::vector<std::string> topics{ "koala-stqf-03",
		"klai-seim-alert-koala-test-03"
	};
	std::string group_id = "whl-consumer-group";

	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	if (conf->set("group.id", group_id, errstr)) {
		std::cout << errstr << std::endl;
		return -1;
	}

	conf->set("bootstrap.servers", brokers, errstr);
	conf->set("max.partition.fetch.bytes", "1024000", errstr);
	//conf->set("enable-auto-commit", "true", errstr);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	tconf->set("auto.offset.reset", "latest", errstr);
	conf->set("default_topic_conf", tconf, errstr);

	RdKafka::KafkaConsumer *m_consumer = RdKafka::KafkaConsumer::create(conf, errstr);
	if (!m_consumer) {
		std::cout << "failed to create consumer " << errstr << std::endl;
		return -1;
	}

#if 0 //从上一次消费结束的位置开始消费
	RdKafka::ErrorCode err = m_consumer->subscribe(topics);
	if (err != RdKafka::ERR_NO_ERROR) {
		std::cout << RdKafka::err2str(err) << std::endl;
		return -1;
	}
#else //指定每个topic的每个分区开始消费的位置

	//基本思路为先获取server端的状态信息，将与订阅相关的topic找出来，根据分区，创建TopicPartion；最后使用assign消费
	RdKafka::Metadata *metadataMap{ nullptr };
	RdKafka::ErrorCode err = m_consumer->metadata(true, nullptr, &metadataMap, 2000);
	if (err != RdKafka::ERR_NO_ERROR) {
		std::cout << RdKafka::err2str(err) << std::endl;
	}
	const RdKafka::Metadata::TopicMetadataVector *topicList = metadataMap->topics();
	std::cout << "broker topic size: " << topicList->size() << std::endl;
	RdKafka::Metadata::TopicMetadataVector subTopicMetaVec;
	std::copy_if(topicList->begin(), topicList->end(), std::back_inserter(subTopicMetaVec), [&topics](const RdKafka::TopicMetadata* data) {
		return std::find_if(topics.begin(), topics.end(), [data](const std::string &tname) {return data->topic() == tname; }) != topics.end();
	});
	std::vector<RdKafka::TopicPartition*> topicpartions;
	std::for_each(subTopicMetaVec.begin(), subTopicMetaVec.end(), [&topicpartions](const RdKafka::TopicMetadata* data) {
		auto parVec = data->partitions();
		std::for_each(parVec->begin(), parVec->end(), [&](const RdKafka::PartitionMetadata *value) {
			std::cout << data->topic() << " has partion: " << value->id() << " Leader is : " << value->leader() << std::endl;
			topicpartions.push_back(RdKafka::TopicPartition::create(data->topic(), value->id(), RdKafka::Topic::OFFSET_END));
		});
	});
	m_consumer->assign(topicpartions);
#endif // 0
	err = m_consumer->subscribe(topics);
	if (err != RdKafka::ERR_NO_ERROR) {
		std::cout << RdKafka::err2str(err) << std::endl;
		return -1;
	}

	while (true)
	{
		RdKafka::Message *msg = m_consumer->consume(6000);
		consume_cb(*msg, nullptr); //消息一条消息
		delete msg;
	}

	return 0;
}

