#include "avro/index.hh"
#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/ValidSchema.hh>
#include <dotenv/dotenv.h>
#include <iostream>
#include <kafka/KafkaConsumer.h>
#include <kafka/KafkaProducer.h>
#include <string>
#include <vector>
using namespace std;
using namespace std::string_literals;

namespace app {

static string errstr = "";

// https://en.cppreference.com/w/cpp/utility/variant/visit
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

std::string join(const std::vector<std::string> &v, std::string delimiter) {
	std::ostringstream ss;

	for (std::vector<std::string>::const_iterator p = v.begin(); p != v.end();
		 ++p) {
		ss << *p;
		if (p != v.end() - 1) {
			ss << delimiter;
		}
	}

	return ss.str();
}

template <typename T, class mapFunction>
auto vector_map(vector<T> data, mapFunction function) {
	using ReturnType = decltype(function(std::declval<T>()));

	vector<ReturnType> result;

	auto size = data.size();
	result.reserve(size);

	for (std::size_t i = 0; i < size; i++) {
		result.push_back(function(data[i]));
	}

	return result;
}

}

#include <libserdes/serdescpp.h>
#include <libserdes/serdescpp-avro.h>

int main(int argc, char **argv) {
	if (argc != 3) {
		std::cerr << "Usage: " << argv[0]
				  << " <librdkafka.config path> <avro path>" << endl;
		return 1;
	}
	auto configPath = argv[1];
	auto avroPath = argv[2];

	dotenv::init(configPath);
	kafka::Properties props({
		{"bootstrap.servers", getenv("bootstrap.servers")},
		{"auto.offset.reset", "earliest"},
		{"transactional.id", "my-transactional-id"},
		{"group.id", "runner-pool"},
		{"isolation.level", "read_committed"},
	});

	auto serdesConf = Serdes::Conf::create();
	serdesConf->set("schema.registry.url", getenv("schema.registry.url"), app::errstr);

	Serdes::Avro<c::Task> *serdesDecoder = Serdes::Avro<c::Task>::create(serdesConf, app::errstr);
	Serdes::Avro<c::TaskResult> *serdesEncoder = Serdes::Avro<c::TaskResult>::create(serdesConf, app::errstr);

	try {
		kafka::clients::KafkaProducer producer(props);
		producer.initTransactions();

		kafka::clients::KafkaConsumer consumer(props);

		set<kafka::Topic> topics = {"calculus", "image-compression", "text-formatting" };
		for (auto topic : topics) {
			std::cout << "% Reading messages from topic: " << topic << std::endl;
			// topics.insert(topic);
		}
		consumer.subscribe(topics);

		while (true) {
			auto records = consumer.poll(std::chrono::milliseconds(1000));
			std::cout << "% " << records.size() << " record(s) fetched"
					  << std::endl;
			producer.beginTransaction();
			for (const auto &record : records) {
				if (record.value().size() == 0) {
					return 0;
				}

				if (!record.error()) {
					std::cout << "% Got a new message..." << std::endl;
					std::cout << "    Topic    : " << record.topic()
							  << std::endl;
					std::cout << "    Partition: " << record.partition()
							  << std::endl;
					std::cout << "    Offset   : " << record.offset()
							  << std::endl;
					std::cout
						<< "    Timestamp: " << record.timestamp().toString()
						<< std::endl;
					std::cout << "    Headers  : "
							  << kafka::toString(record.headers()) << std::endl;
					std::cout << "    Key   [" << record.key().toString() << "]"
							  << std::endl;
					auto value = record.value();
					std::cout << "    Value [" << value.toString() << "]"
							  << std::endl;

					c::Task *task;
					c::Task *task2 = new c::Task();
					// cout << "Step 1: " << task << endl;
					Serdes::Schema *_schema = NULL;
					auto dsize = serdesDecoder->deserialize(&_schema, &task, value.data(), value.size(), app::errstr);
					auto result = std::visit(app::overloaded {
						[](const c::Calculus &calculus) {
							c::CalculusResult result;
							result.expression = calculus.expression + " resolved";
							return c::TaskResult(result);
						},
						[](const c::ImageCompression &imageCompression) {
							c::ImageCompressionResult result;
							result.directory = app::join(imageCompression.url, "_") + " (compressed)"s;
							return c::TaskResult(result);
						},
						[](const c::TextFormatting &textFormatting) {
							c::TextFormattingResult result;
							result.formatted = textFormatting.text + " (formatted with " +
											textFormatting.formatting + ")";
							return c::TaskResult(result);
						},
					}, *task);

					auto producerTopic = record.topic() + "-result"s;
					auto schema = Serdes::Schema::get(serdesEncoder, producerTopic + "-value", app::errstr);
					if (!schema) {
      					throw new runtime_error("Failed to get schema: " + app::errstr);
					}
					vector<char> out;
					serdesEncoder->serialize(schema, &result, out, app::errstr);
					kafka::Value returnValue(out.data(), out.size());
					kafka::Key returnKey;

					kafka::clients::producer::ProducerRecord producerRecord(
						producerTopic, returnKey, returnValue);

					auto recordHeaders = record.headers();
					auto clientIdHeaderIterator = std::find_if(
						recordHeaders.begin(), recordHeaders.end(),
						[](kafka::Header hh) { return hh.key == "client-id"; });
					if (clientIdHeaderIterator != recordHeaders.end()) {
						auto clientIdHeader = *clientIdHeaderIterator;
						producerRecord.headers().push_back(clientIdHeader);
					}

					producer.syncSend(producerRecord);
				} else {
					// Errors are typically informational, thus no special
					// handling is required
					std::cerr << record.toString() << std::endl;
				}
			}
			auto topicPartitions = app::vector_map(
				records, [](kafka::clients::consumer::ConsumerRecord record) {
					return pair{record.topic(), record.partition()};
				});
			producer.sendOffsetsToTransaction(
				consumer.endOffsets(
					set(topicPartitions.begin(), topicPartitions.end())),
				consumer.groupMetadata(), std::chrono::milliseconds(60000));
			producer.commitTransaction();
		}
	} catch (const std::exception &e) {
		cerr << "Exception occured: " << e.what() << endl;
	}
	delete serdesConf;
	delete serdesDecoder;
	delete serdesEncoder;
}
