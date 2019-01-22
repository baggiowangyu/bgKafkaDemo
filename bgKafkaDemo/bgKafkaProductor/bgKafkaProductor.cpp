// bgKafkaProductor.cpp : 定义控制台应用程序的入口点。
// https://blog.csdn.net/caoshangpa/article/details/79786100

#include "stdafx.h"
#include <iostream>
#include <string>
#include <signal.h>

#include "rdkafkacpp.h"

static bool run = true;

static void sigterm (int sig) {
	run = false;
}


class bgDeliveryReportCb : public RdKafka::DeliveryReportCb
{
public:
	void dr_cb(RdKafka::Message &message)
	{
		std::cout<<"Message delivery for ("<<message.len()<<" bytes):"<<message.errstr()<<std::endl;

		if (message.key())
		{
			std::cout<<"key:"<<message.key()->c_str()<<";"<<std::endl;
		}
	}
};

class bgEventCb : public RdKafka::EventCb
{
public:
	void event_cb(RdKafka::Event &event)
	{
		switch (event.type())
		{
		case RdKafka::Event::EVENT_ERROR:
			std::cerr<<"ERROR("<<RdKafka::err2str(event.err())<<"):"<<event.str()<<std::endl;
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
			{
				run = false;
			}
			break;

		case RdKafka::Event::EVENT_STATS:
			std::cerr<<"\"STATS\":"<<event.str()<<std::endl;
			break;

		case RdKafka::Event::EVENT_LOG:
			fprintf(stderr, "LOG-%i-%s:%s\n", event.severity(), event.fac().c_str(), event.str().c_str());
			break;

		default:
			std::cerr<<"EVENT "<<event.type()<<"("<<RdKafka::err2str(event.err())<<"):"<<event.str()<<std::endl;
			break;
		}
	}
};

int _tmain(int argc, _TCHAR* argv[])
{
	if (argc < 4)
	{
		std::cout<<"usage:"<<std::endl;
		std::cout<<"bgKafkaProductor.exe [kafka-ip] [kafka-port] [kafka-topic]"<<std::endl;
		system("pause");
		return 0;
	}

	std::string brokers = argv[1];
	std::string topic_str = argv[3];
	int32_t partition = RdKafka::Topic::PARTITION_UA;

	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	std::string errstr;
	conf->set("bootstrap.servers", brokers, errstr);

	bgEventCb bg_event_cb;
	conf->set("event_cb", &bg_event_cb, errstr);

	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);

	bgDeliveryReportCb bg_dr_cb;
	conf->set("dr_cb", &bg_dr_cb, errstr);

	// 生产者对象
	RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
	if (!producer)
	{
		std::cerr<<"Failed to create producer : "<<errstr<<std::endl;
		system("pause");
		return 0;
	}

	std::cout<<"% Created producer "<<producer->name()<<std::endl;

	// 主题
	RdKafka::Topic *topic = RdKafka::Topic::create(producer, topic_str, topic_conf, errstr);
	if (!topic)
	{
		std::cerr<<"Failed to create topic : "<<errstr<<std::endl;
		system("pause");
		return 0;
	}

	for (std::string line; run && std::getline(std::cin, line);)
	{
		if (line.empty())
		{
			producer->poll(0);
			continue;
		}

		RdKafka::ErrorCode resp = producer->produce(topic, partition, RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(line.c_str()), line.size(), NULL, NULL);
		if (resp != RdKafka::ERR_NO_ERROR)
			std::cerr<<"% Producer failed: "<<RdKafka::err2str(resp)<<std::endl;
		else
			std::cerr<<"% Producer message ("<<line.size()<<") bytes)"<<std::endl;

		producer->poll(0);
	}

	run = true;

	while (run && producer->outq_len() > 0)
	{
		std::cerr<<"Waiting for "<<producer->outq_len()<<std::endl;
		producer->poll(1000);
	}

	delete conf;
	delete topic_conf;
	delete topic;
	delete producer;

	RdKafka::wait_destroyed(5000);

	return 0;
}

