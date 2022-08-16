
#include <mpi.h>
// import in this order
#include "./mpi-runner.hh"
#include "./mpi.hh"
#include <libserdes/serdescpp.h>
#include <libserdes/serdescpp-avro.h>

#include "avro/Calculus.hh"
#include "avro/CalculusResult.hh"
#include "avro/ErrorResult.hh"
#include "avro/WordCount.hh"
#include "avro/WordCountResult.hh"
#include "avro/ImageCompression.hh"
#include "avro/ImageCompressionResult.hh"

#include <avro/Compiler.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/ValidSchema.hh>
#include <dotenv/dotenv.h>
#include <iostream>
#include <kafka/KafkaConsumer.h>
#include <kafka/KafkaProducer.h>
#include <regex>
#include <string>
#include <variant>
#include <vector>
using namespace std;
using namespace std::string_literals;


namespace c {
    using Task = std::variant<c::Calculus, c::ImageCompression, c::WordCount>;
    using TaskResult = std::variant<c::CalculusResult, c::ImageCompressionResult, c::WordCountResult>;
}

namespace avro {
	template <typename... TArgs>
	struct codec_traits<std::variant<TArgs...>> {
		static void encode(Encoder& e, const std::variant<TArgs...>& v) {
			std::visit([&e](auto &&arg) { avro::encode(e, arg); }, v);
		}
		static void decode(Decoder& d, std::variant<TArgs...>& v) {
			std::visit([&d](auto &&arg) { avro::decode(d, arg); }, v);
		}
	};
}

namespace app {

string errstr = "";

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


int main(int argc, char **argv) {
	MPI_Init(NULL, NULL);

	MPI_Comm_rank(MPI_COMM_WORLD, &mpi::comm_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &mpi::comm_size);

    mpi_runner::init();

	Serdes::Conf *serdesConf;
	Serdes::Avro<c::Task> *serdesDecoder;
	Serdes::Avro<c::TaskResult> *serdesEncoder;
	kafka::clients::KafkaProducer *producer;
	kafka::clients::KafkaConsumer *consumer;
	try {
		ROOT_ONLY {
			if (argc != 2) {
				std::cerr << "Usage: " << argv[0]
						<< " <librdkafka.config path> <avro path>" << endl;
				return 1;
			}
			auto configPath = argv[1];

			dotenv::init(configPath);

			kafka::Properties props({
				{"bootstrap.servers", getenv("bootstrap.servers")},
				{"transactional.id", "my-transactional-id"},
				{"auto.offset.reset", "earliest"},
				{"group.id", "runner-pool"},
				{"max.poll.records", "1"},
				{"isolation.level", "read_committed"},
			});

			serdesConf = Serdes::Conf::create();
			serdesConf->set("schema.registry.url", getenv("schema.registry.url"), app::errstr);

			serdesDecoder = Serdes::Avro<c::Task>::create(serdesConf, app::errstr);
			serdesEncoder = Serdes::Avro<c::TaskResult>::create(serdesConf, app::errstr);
			producer = new kafka::clients::KafkaProducer(props);
			producer->initTransactions();

			consumer = new kafka::clients::KafkaConsumer(props);

			set<kafka::Topic> topics = {"calculus", "image-compression", "word-count" };
			for (auto topic : topics) {
				std::cout << "% Reading messages from topic: " << topic << std::endl;
			}
			consumer->subscribe(topics);
		}
		while (true) {
			ROOT_ONLY {
				auto records = consumer->poll(std::chrono::milliseconds(1000));
				std::cout << "% " << records.size() << " record(s) fetched"
						<< std::endl;
				try {
					producer->beginTransaction();
					if (records.empty()) {
						producer->abortTransaction();
						continue;
					}
					// only one by consumer configuration
					const auto &record = records[0];
					if (record.value().size() == 0) {
						return 2;
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

						c::Task task;
						auto recordTopic = record.topic();
						if (recordTopic == "calculus") {
							task = c::Calculus();
						} else if (recordTopic == "image-compression") {
							task = c::ImageCompression();
						} else if (recordTopic == "word-count") {
							task = c::WordCount();
						} else {
							throw runtime_error("Consumer type unknown");
						}
						Serdes::Schema *schemaD = nullptr;
						auto dsize = serdesDecoder->deserialize(&schemaD, &task, value.data(), value.size(), app::errstr);
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
							[](const c::WordCount &wordCount) {
								c::WordCountResult result;
								result.counts = mpi_runner::run_word_count(wordCount.url);
								return c::TaskResult(result);
							},
						}, task);

						auto producerTopic = record.topic() + "-result"s;
						auto schemaE = Serdes::Schema::get(serdesEncoder, producerTopic + "-value", app::errstr);
						if (!schemaE) {
							throw new runtime_error("Failed to get schemaE: " + app::errstr);
						}
						vector<char> out;
						serdesEncoder->serialize(schemaE, &result, out, app::errstr);
						kafka::Value returnValue(out.data(), out.size());
						kafka::Key returnKey;

						std::cout << "    Producer topic [" << producerTopic << "]" << endl;

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

						producer->syncSend(producerRecord);
					} else {
						// Errors are typically informational, thus no special
						// handling is required
						std::cerr << record.toString() << std::endl;
					}

					auto topicPartitions = app::vector_map(
						records, [](kafka::clients::consumer::ConsumerRecord record) {
							return pair{record.topic(), record.partition()};
						});
					producer->sendOffsetsToTransaction(
						consumer->endOffsets(
							set(topicPartitions.begin(), topicPartitions.end())),
						consumer->groupMetadata(), std::chrono::milliseconds(60000));
					producer->commitTransaction();
				} catch(const kafka::KafkaException& e) {
					std::cerr << e.what() << endl;
					producer->abortTransaction();
				}
			}
			NON_ROOT_ONLY {
				mpi_runner::run_non_root();
			}
		}
	} catch (const std::exception &e) {
		cerr << "Fatal exception occured: " << e.what() << endl;
	}
	ROOT_ONLY {
		delete serdesConf;
		delete serdesDecoder;
		delete serdesEncoder;
		delete producer;
		delete consumer;
	}

	mpi_runner::finalize();

	MPI_Finalize();
}
