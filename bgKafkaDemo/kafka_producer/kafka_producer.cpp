// kafka_producer.cpp : 定义控制台应用程序的入口点。
//

#include "stdafx.h" 
#include <iostream>
#include <thread>
#include "rdkafkacpp.h"  


int _tmain(int argc, _TCHAR* argv[])
{
	//std::string brokers = "127.0.0.1:9092,192.168.0.128:9092,192.168.0.129:9092";
	std::string brokers = "127.0.0.1:9092";
	std::string errorStr;
	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	if (!conf) {
		std::cout << "Create RdKafka Conf failed" << std::endl;
		return -1;
	}

	conf->set("message.max.bytes", "10240000", errorStr); //最大字节数
	conf->set("replica.fetch.max.bytes", "20485760", errorStr);
	conf->set("bootstrap.servers", brokers, errorStr);

	RdKafka::Producer *producer = RdKafka::Producer::create(conf, errorStr);
	if (!producer) {
		std::cout << "Create Producer failed" << std::endl;
		return -1;
	}
	//创建Topic
	RdKafka::Topic *topic = RdKafka::Topic::create(producer, "koala-stqf-03", tconf, errorStr);
	if (!topic) {
		std::cout << "Create Topic failed" << std::endl;
	}

	while (true)
	{   //发送消息
		RdKafka::ErrorCode resCode = producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, "123456789", 10, nullptr, nullptr);
		if (resCode != RdKafka::ERR_NO_ERROR) {
			std::cerr << "Produce failed: " << RdKafka::err2str(resCode) << std::endl;
		}

		
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}

	delete conf;
	delete tconf;
	delete topic;
	delete producer;

	RdKafka::wait_destroyed(5000);

	return 0;
}

