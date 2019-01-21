// bgKafkaProductor.cpp : 定义控制台应用程序的入口点。
// https://blog.csdn.net/caoshangpa/article/details/79786100

#include "stdafx.h"
#include <iostream>
#include <string>

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
		}
	}
};

int _tmain(int argc, _TCHAR* argv[])
{
	return 0;
}

