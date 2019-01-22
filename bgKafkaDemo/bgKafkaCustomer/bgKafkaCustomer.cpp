// bgKafkaCustomer.cpp : 定义控制台应用程序的入口点。
//

#include "stdafx.h"
#include <iostream>
#include <string>
#include <signal.h>

#include "rdkafkacpp.h"

static bool run = true;
static bool exit_eof = true;
static int eof_cnt = 0;
static int partition_cnt = 0;
static int verbosity = 1;
static long msg_cnt = 0;
static int64_t msg_bytes = 0;

static void sigterm(int sig) {
	run = false;
}

class bgEventCb : public RdKafka::EventCb {
public:
	void event_cb(RdKafka::Event &event) {
		switch (event.type())
		{
		case RdKafka::Event::EVENT_ERROR:
			std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
				event.str() << std::endl;
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
				run = false;
			break;

		case RdKafka::Event::EVENT_STATS:
			std::cerr << "\"STATS\": " << event.str() << std::endl;
			break;

		case RdKafka::Event::EVENT_LOG:
			fprintf(stderr, "LOG-%i-%s: %s\n",
				event.severity(), event.fac().c_str(), event.str().c_str());
			break;

		case RdKafka::Event::EVENT_THROTTLE:
			std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
				event.broker_name() << " id " << (int)event.broker_id() << std::endl;
			break;

		default:
			std::cerr << "EVENT " << event.type() <<
				" (" << RdKafka::err2str(event.err()) << "): " <<
				event.str() << std::endl;
			break;
		}
	}
};

void msg_consume(RdKafka::Message* message, void* opaque) {
	switch (message->err()) {
	case RdKafka::ERR__TIMED_OUT:
		//std::cerr << "RdKafka::ERR__TIMED_OUT"<<std::endl;
		break;

	case RdKafka::ERR_NO_ERROR:
		/* Real message */
		msg_cnt++;
		msg_bytes += message->len();
		if (verbosity >= 3)
			std::cerr << "Read msg at offset " << message->offset() << std::endl;
		RdKafka::MessageTimestamp ts;
		ts = message->timestamp();
		if (verbosity >= 2 &&
			ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
			std::string tsname = "?";
			if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
				tsname = "create time";
			else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
				tsname = "log append time";
			std::cout << "Timestamp: " << tsname << " " << ts.timestamp << std::endl;
		}
		if (verbosity >= 2 && message->key()) {
			std::cout << "Key: " << *message->key() << std::endl;
		}
		if (verbosity >= 1) {
			printf("%.*s\n",
				static_cast<int>(message->len()),
				static_cast<const char *>(message->payload()));
		}
		break;

	case RdKafka::ERR__PARTITION_EOF:
		/* Last message */
		if (exit_eof && ++eof_cnt == partition_cnt) {
			std::cerr << "%% EOF reached for all " << partition_cnt <<
				" partition(s)" << std::endl;
			run = false;
		}
		break;

	case RdKafka::ERR__UNKNOWN_TOPIC:
	case RdKafka::ERR__UNKNOWN_PARTITION:
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
		break;

	default:
		/* Errors */
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
	}
}

class bgConsumeCb : public RdKafka::ConsumeCb {
public:
	void consume_cb(RdKafka::Message &msg, void *opaque) {
		msg_consume(&msg, opaque);
	}
};


int _tmain(int argc, _TCHAR* argv[])
{
	std::string brokers = "localhost";
	std::string errstr;
	std::string topic_str = "topic_wy";
	std::vector<std::string> topics;
	std::string group_id = "101";

	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	//group.id必须设置
	if (conf->set("group.id", group_id, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << errstr << std::endl;
		exit(1);
	}

	topics.push_back(topic_str);
	//bootstrap.servers可以替换为metadata.broker.list
	conf->set("metadata.broker.list", brokers, errstr);

	bgConsumeCb bg_consume_cb;
	conf->set("consume_cb", &bg_consume_cb, errstr);

	bgEventCb bg_event_cb;
	conf->set("event_cb", &bg_event_cb, errstr);
	conf->set("default_topic_conf", tconf, errstr);

	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);

	RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
	if (!consumer) {
		std::cerr << "Failed to create consumer: " << errstr << std::endl;
		exit(1);
	}
	//std::cout << "% Created consumer " << consumer->name() << std::endl;

	RdKafka::ErrorCode err = consumer->subscribe(topics);
	if (err) {
		std::cerr << "Failed to subscribe to " << topics.size() << " topics: "
			<< RdKafka::err2str(err) << std::endl;
		exit(1);
	}

	while (run) {
		//5000毫秒未订阅到消息，触发RdKafka::ERR__TIMED_OUT
		RdKafka::Message *msg = consumer->consume(5000);
		msg_consume(msg, NULL);
		delete msg;
	}

	consumer->close();

	delete conf;
	delete tconf;
	delete consumer;

	std::cerr << "% Consumed " << msg_cnt << " messages ("
		<< msg_bytes << " bytes)" << std::endl;

	//应用退出之前等待rdkafka清理资源
	RdKafka::wait_destroyed(5000);

	return 0;
}

