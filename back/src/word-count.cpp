#include <algorithm>
#include <cctype>
#include <cpr/cpr.h>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mpi.h>
#include <numeric>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

using namespace std;

#define ROOT 0
#define WORD_BUFFER_SIZE 64

struct word_count {
	char word[WORD_BUFFER_SIZE];
	int count;
	friend ostream &operator<<(ostream &os, const word_count &wc) {
		os << string(wc.word, WORD_BUFFER_SIZE) << ": " << wc.count;
		return os;
	}
};

ostream &
operator<<(ostream &os,
		   const std::pair<const std::__cxx11::basic_string<char>, int> pp) {
	os << pp.first << ": " << pp.second;
	return os;
}

static int comm_rank, comm_size;

#define LOG(values...)                                                         \
	do {                                                                       \
		cout << "[Rank " << comm_rank << " / " << comm_size - 1 << "] "        \
			 << values;                                                        \
	} while (0);

#define LOGLN(values...) LOG(values << '\n')

void log(auto const &rem, auto const &values) {
	LOGLN(rem);
	for (auto const &v : values) {
		cout << " " << v << '\n';
	}
	cout.flush();
}

#define OFFSETOF(type, field) ((unsigned long)&(((type *)0)->field))

vector<string> get_word_tokens(string input) {
	std::regex rgx("((?:\\w|[-_])+)");
	std::sregex_token_iterator begin(input.begin(), input.end(), rgx, 1), end;
	vector<string> v;
	std::copy(begin, end, std::back_inserter(v));
	return v;
}

int main(int argc, char **argv) {
	std::ios_base::sync_with_stdio(false);

	MPI_Init(NULL, NULL);

	MPI_Comm_rank(MPI_COMM_WORLD, &comm_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

	if (argc < 2) {
		cout << "Arguments forgotten" << endl;
		exit(-1);
	}

	MPI_Datatype mpi_word_count;
#define STRUCT_LEN 2
	int array_of_block_lengths[STRUCT_LEN] = {WORD_BUFFER_SIZE, 1};
	MPI_Aint array_of_displacements[STRUCT_LEN] = {OFFSETOF(word_count, word),
												   OFFSETOF(word_count, count)};
	MPI_Datatype array_of_types[STRUCT_LEN] = {MPI_CHAR, MPI_INT};
	MPI_Type_create_struct(STRUCT_LEN, array_of_block_lengths,
						   array_of_displacements, array_of_types,
						   &mpi_word_count);
	MPI_Type_commit(&mpi_word_count);

	map<string, int> table;
	for (int i = 0; i * comm_size + comm_rank < argc - 1; i++) {
		string url = *(argv + i * comm_size + comm_rank + 1);
		auto r = cpr::Get(cpr::Url{url});
		if (r.status_code < 200 || r.status_code > 299) {
			continue;
		}
		istringstream in(r.text);
		std::string line;
		while (std::getline(in, line)) {
			auto words = get_word_tokens(line);
			for (auto _word : words) {
				auto word = _word;
				std::transform(word.begin(), word.end(), word.begin(),
							   [](char cc) { return std::tolower(cc); });
				table[word]++;
			}
		}
	}
	auto beg = table.begin();
	auto partitions =
		vector<vector<word_count>>(comm_size, vector<word_count>());
	std::hash<string> hasher;
	for (size_t i = 0; beg != table.end(); beg++, i++) {
		const auto [word, count] = *beg;
		word_count item;
		item.count = count;
		strncpy(item.word, word.c_str(), WORD_BUFFER_SIZE - 1);
		auto rank = hasher(item.word) % comm_size;
		auto &partition = partitions[rank];
		partition.push_back(item);
	}

	auto recvbuf_sizes = new int[comm_size];
	for (int rank = 0; rank < comm_size; rank++) {
		auto paritionSize = partitions[rank].size();
		MPI_Gather(&paritionSize, 1, MPI_INT, recvbuf_sizes, 1, MPI_INT, rank,
				   MPI_COMM_WORLD);
	}

	auto recvbuf_size =
		std::accumulate(recvbuf_sizes, recvbuf_sizes + comm_size, 0);
	auto recvbuf = new word_count[recvbuf_size];
	for (int rank = 0; rank < comm_size; rank++) {
		auto partition = partitions[rank];
		auto displs = new int[comm_size]{0};
		for (size_t i = 1; i < comm_size; i++) {
			displs[i] = displs[i - 1] + recvbuf_sizes[i - 1];
		}
		MPI_Gatherv(partition.data(), partition.size(), mpi_word_count, recvbuf,
					recvbuf_sizes, displs, mpi_word_count, rank,
					MPI_COMM_WORLD);
		delete displs;
	}

	table.clear();
	for (int i = 0; i < recvbuf_size; i++) {
		auto item = recvbuf[i];
		table[item.word] += item.count;
	}
	delete recvbuf;

	auto completePartitionSize = table.size();
	auto completePartition = vector<word_count>(completePartitionSize);
	beg = table.begin();
	std::ios_base::sync_with_stdio(false);
	for (size_t i = 0; beg != table.end(); beg++, i++) {
		const auto [word, count] = *beg;
		word_count item;
		item.count = count;
		strncpy(item.word, word.c_str(), WORD_BUFFER_SIZE - 1);
		completePartition[i] = item;
	}

	MPI_Gather(&completePartitionSize, 1, MPI_INT, recvbuf_sizes, 1, MPI_INT, ROOT, MPI_COMM_WORLD);
	if (comm_rank == ROOT) {
		recvbuf_size = std::accumulate(recvbuf_sizes, recvbuf_sizes + comm_size, 0);
		recvbuf = new word_count[recvbuf_size];
	}
	auto displs = new int[comm_size]{0};
	for (size_t i = 1; i < comm_size; i++) {
		displs[i] = displs[i - 1] + recvbuf_sizes[i - 1];
	}
	MPI_Gatherv(completePartition.data(), completePartition.size(), mpi_word_count, recvbuf, recvbuf_sizes, displs, mpi_word_count, ROOT, MPI_COMM_WORLD);
	if (comm_rank == ROOT) {
		table.clear();
		for (int i = 0; i < recvbuf_size; i++) {
			auto item = recvbuf[i];
			table[item.word] += item.count;
		}
		log("recvbuf", table);
	}
	delete displs;
	delete recvbuf_sizes;

	MPI_Type_free(&mpi_word_count);
	MPI_Finalize();
}
