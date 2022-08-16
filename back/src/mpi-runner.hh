#pragma once

#include <algorithm>
#include <cpr/cpr.h>
#include <cctype>
#include <iostream>
#include <iterator>
#include <mpi.h>
#include <numeric>
#include <regex>
#include <string>

namespace mpi_runner {

const int WORD_BUFFER_SIZE = 64;

MPI_Datatype mpi_word_count;

struct word_count {
	char word[WORD_BUFFER_SIZE];
	int count;
	friend std::ostream &operator<<(std::ostream &os, const word_count &wc) {
		os << std::string(wc.word, WORD_BUFFER_SIZE) << ": " << wc.count;
		return os;
	}
};

std::ostream &operator<<(std::ostream &os,
		   const std::pair<const std::string, int> pp) {
	os << pp.first << ": " << pp.second;
	return os;
}

std::vector<std::string> get_word_tokens(std::string input) {
	std::regex rgx("((?:\\w|[-_])+)");
	std::sregex_token_iterator begin(input.begin(), input.end(), rgx, 1), end;
	std::vector<std::string> v;
	std::copy(begin, end, std::back_inserter(v));
	return v;
}

}

using mpi_runner::operator<<;

#include "./mpi.hh"

namespace mpi_runner {

using namespace std;

void init() {
	const int STRUCT_LEN = 2;
	int array_of_block_lengths[STRUCT_LEN] = {WORD_BUFFER_SIZE, 1};
	MPI_Aint array_of_displacements[STRUCT_LEN] = {OFFSETOF(word_count, word),
												   OFFSETOF(word_count, count)};
	MPI_Datatype array_of_types[STRUCT_LEN] = {MPI_CHAR, MPI_INT};
	MPI_Type_create_struct(STRUCT_LEN, array_of_block_lengths,
						   array_of_displacements, array_of_types,
						   &mpi_word_count);
	MPI_Type_commit(&mpi_word_count);
}

void finalize() {
	MPI_Type_free(&mpi_word_count);
}

enum class Task : int {
	WORD_COUNT
};

map<string, int> run_word_count(vector<string> _urls);

void run_non_root() {
	Task type;
	MPI_Bcast(&type, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
	switch (type) {
		case Task::WORD_COUNT:
			run_word_count(vector<string>());
			break;
		default:
			break;
	}
}

map<string, int> run_word_count(vector<string> _urls) {
	ROOT_ONLY {
		auto type = Task::WORD_COUNT;
		MPI_Bcast(&type, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
	}

	size_t urlCount;
	ROOT_ONLY {
		urlCount = _urls.size();
	}
	MPI_Bcast(&urlCount, 1, MPI_LONG, ROOT, MPI_COMM_WORLD);
	auto urlSizes = new int[urlCount];
	auto urls = new char*[urlCount];
	ROOT_ONLY {
		int i = 0;
		for(auto url : _urls) {
			// + 1 for '\0'
			auto urlSize = url.length() + 1;
			urlSizes[i] = urlSize;
			urls[i] = new char[urlSize];
			strncpy(urls[i], url.data(), urlSize - 1);
			urls[i][urlSize - 1] = '\0';
			i++;
		}
	}
	MPI_Bcast(urlSizes, urlCount, MPI_INT, ROOT, MPI_COMM_WORLD);
	NON_ROOT_ONLY {
		for (int i = 0; i < urlCount; i++) {
			urls[i] = new char[urlSizes[i]];
		}
	}
	for (int i = 0; i < urlCount; i++) {
		MPI_Bcast(urls[i], urlSizes[i], MPI_CHAR, ROOT, MPI_COMM_WORLD);
	}
	map<string, int> table;
	for (int i = 0, j = 0; (j = i * mpi::comm_size + mpi::comm_rank) < urlCount; i++) {
		string url = urls[j];
		auto response = cpr::Get(cpr::Url{url});
		if (response.status_code < 200 || response.status_code > 299) {
			continue;
		}
        auto words = get_word_tokens(response.text);
        for (auto _word : words) {
            auto word = _word;
            std::transform(word.begin(), word.end(), word.begin(),
                            [](char cc) { return std::tolower(cc); });
            table[word]++;
        }
	}
	auto beg = table.begin();
	auto partitions =
		vector<vector<word_count>>(mpi::comm_size, vector<word_count>());
	std::hash<string> hasher;
	for (size_t i = 0; beg != table.end(); beg++, i++) {
		const auto [word, count] = *beg;
		word_count item;
		item.count = count;
		strncpy(item.word, word.c_str(), WORD_BUFFER_SIZE - 1);
		auto rank = hasher(item.word) % mpi::comm_size;
		auto &partition = partitions[rank];
		partition.push_back(item);
	}

	auto recvbuf_sizes = new int[mpi::comm_size];
	for (int rank = 0; rank < mpi::comm_size; rank++) {
		auto paritionSize = partitions[rank].size();
		MPI_Gather(&paritionSize, 1, MPI_INT, recvbuf_sizes, 1, MPI_INT, rank,
				   MPI_COMM_WORLD);
	}
	auto recvbuf_size =
		std::accumulate(recvbuf_sizes, recvbuf_sizes + mpi::comm_size, 0);
	auto recvbuf = new word_count[recvbuf_size];
	for (int rank = 0; rank < mpi::comm_size; rank++) {
		auto partition = partitions[rank];
		auto displs = mpi::get_displs(recvbuf_sizes, mpi::comm_size);
		MPI_Gatherv(partition.data(), partition.size(), mpi_word_count, recvbuf,
					recvbuf_sizes, displs.data(), mpi_word_count, rank,
					MPI_COMM_WORLD);
	}

	auto fill_table = [](map<string, int> &table, word_count *recvbuf, int recvbuf_size) {
		table.clear();
		for (int i = 0; i < recvbuf_size; i++) {
			auto item = recvbuf[i];
			table[item.word] += item.count;
		}
		delete recvbuf;
	};
	fill_table(table, recvbuf, recvbuf_size);

	auto completePartitionSize = table.size();
	auto completePartition = vector<word_count>(completePartitionSize);
	beg = table.begin();
	for (size_t i = 0; beg != table.end(); beg++, i++) {
		const auto [word, count] = *beg;
		word_count item;
		item.count = count;
		strncpy(item.word, word.c_str(), WORD_BUFFER_SIZE - 1);
		completePartition[i] = item;
	}

	MPI_Gather(&completePartitionSize, 1, MPI_INT, recvbuf_sizes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
	ROOT_ONLY {
		recvbuf_size = std::accumulate(recvbuf_sizes, recvbuf_sizes + mpi::comm_size, 0);
		recvbuf = new word_count[recvbuf_size];
	}
	auto displs = mpi::get_displs(recvbuf_sizes, mpi::comm_size);
	MPI_Gatherv(completePartition.data(), completePartition.size(), mpi_word_count, recvbuf, recvbuf_sizes, displs.data(), mpi_word_count, ROOT, MPI_COMM_WORLD);
	ROOT_ONLY {
		fill_table(table, recvbuf, recvbuf_size);
	}
	delete[] recvbuf_sizes;
	return table;
}

}

