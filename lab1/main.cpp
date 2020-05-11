#include <iostream>
#include <thread>
#include <bits/stdc++.h>
#include <atomic>

#define cost (now.tv_sec-st.tv_sec)*1000000000+(now.tv_nsec-st.tv_nsec)

using namespace std;

int thread_num;
atomic_int txn_num(0);
map<string,int> storage;
pair<int,int> row[10][1000000];
int row_num = 0;
int row_tot[10];
mutex wr_lock;
int rv[10], _rv[10][1000000]; 
mutex rv_lock;
thread td[10];
struct timespec st;


void run(int tid) {
	ifstream fin;
	ofstream fout;
	// ofstream debug;
	fin.open(("thread_"s + std::to_string(tid) + ".txt"s).c_str(), ios::in);
	fout.open(("output_thread_"s + std::to_string(tid) + ".csv"s).c_str(), ios::out);
	// debug.open(("debug_"s + std::to_string(tid) + ".csv"s).c_str(), ios::out);
	fout << "transaction_id,type,time,value" << endl;
	struct timespec now;
	string command, name, _name, op; int txn_id, val; bool get_lock = false;
	while (fin >> command >> txn_id) {
		if (command != "BEGIN"s) break;
		rv_lock.lock();
		int timestamp = ++txn_num;
		for (uint8_t i = 1; i <= thread_num; i++) if (rv[i]) _rv[tid][rv[i]] = timestamp;
		rv[tid] = timestamp;
		clock_gettime(CLOCK_REALTIME, &now);
		fout << txn_id << ",BEGIN," << cost << "," << endl;
		rv_lock.unlock();
		fin >> command;
		while (command != "COMMIT"s) {
			if (command == "READ"s) {
				fin >> name;
				int row_id = storage[name];
				int idx = row_tot[row_id];
				while (true) {
					idx -= 1;
					if (row[row_id][idx].second > timestamp) continue;
					if (_rv[tid][row[row_id][idx].second] == timestamp) continue;
					// debug << _rv[tid][row[row_id][idx].second] << "," << timestamp << "," << row[row_id][idx].second << endl;
					break;
				}
				clock_gettime(CLOCK_REALTIME, &now);
				fout << txn_id << "," << name << "," << cost << "," << row[row_id][idx].first << endl;
				// debug << txn_id << "," << name << "," << cost << "," << row[row_id][idx].first << "," << row[row_id][idx].second << "," << idx << "," << row_tot[row_id] << endl;
				// fout << idx << endl;
			}
			if (command == "SET"s) {
				fin >> _name >> name >> op >> val; _name = _name.substr(0,_name.length()-1); 
				int row_id = storage[name];
				int idx = row_tot[row_id];
				while (true) {
					idx -= 1;
					if (row[row_id][idx].second > timestamp) continue;
					if (_rv[tid][row[row_id][idx].second] == timestamp) continue;
					break;
				}
				if (get_lock == false) wr_lock.lock();
				get_lock = true;
				row[row_id][row_tot[row_id]] = make_pair(row[row_id][idx].first + val * (op == "+"s ? +1 : -1), timestamp);
				row_tot[row_id] += 1;
				// clock_gettime(CLOCK_REALTIME, &now);
				// debug << txn_id << "," << name << "," << cost << "," << row[row_id][idx].first << "," << row_tot[row_id] << "," << idx << "x" << timestamp << endl;
				// fout << row_tot[row_id]-1 << endl;
				// wr_lock[row_id].unlock();
			}
			fin >> command;
		}
		rv_lock.lock();
		rv[tid] = 0;
		if (get_lock) wr_lock.unlock();
		get_lock = false;
		clock_gettime(CLOCK_REALTIME, &now);
		fout << txn_id << ",END," << cost << "," << endl;
		rv_lock.unlock();
		fin >> val;
	}
	fin.close();
}

inline void prepare() {
	ifstream pstream;
	pstream.open("data_prepare.txt", ios::in);
	string command, name; int val;
	while (pstream >> command >> name >> val) {
		row_tot[row_num] = 0;
		row[row_num][row_tot[row_num]++] = make_pair(val, 0);
		storage[name] = row_num++;
	}
	pstream.close();
}

int main(int argc, const char *argv[]) {
    if (argc >= 2) {
		sscanf(argv[1], "%d", &thread_num);
	} else {
		thread_num = 1;
	}
	prepare();
	clock_gettime(CLOCK_REALTIME, &st);
	for (uint8_t i = 1; i <= thread_num; i++) {
		td[i] = thread(run, i);
	}
	for (uint8_t i = 1; i <= thread_num; i++) {
		td[i].join();
	}
	return 0;
}