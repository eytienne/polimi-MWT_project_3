#pragma once

namespace mpi {

int comm_rank, comm_size;

#define ROOT 0
#define ROOT_ONLY if (mpi::comm_rank == ROOT)
#define NON_ROOT_ONLY if (mpi::comm_rank != ROOT)

#define LOG(values...)                                                         \
	do {                                                                       \
		std::cout << "[Rank " << mpi::comm_rank << " / " << mpi::comm_size - 1 << "] "        \
			 << values;                                                        \
	} while (0);

#define LOGLN(values...) LOG(values << '\n')

void log(auto const &rem, auto const &values) {
	LOGLN(rem);
	for (auto const &v : values) {
		std::cout << " " << v << '\n';
	}
	std::cout.flush();
}

#define OFFSETOF(type, field) ((long)&(((type *)0)->field))

std::vector<int> get_displs(const int *counts, int countsLen) {
	auto displs = std::vector<int>(countsLen);
	for (size_t i = 1; i < countsLen; i++) {
		displs[i] = displs[i - 1] + counts[i - 1];
	}
	return displs;
}

}