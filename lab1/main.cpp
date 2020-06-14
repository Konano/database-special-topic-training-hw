#include <iostream>
#include <thread>
#include <bits/stdc++.h>
#include <atomic>

#define cost (now.tv_sec-st.tv_sec)*1000000000+(now.tv_nsec-st.tv_nsec)

using namespace std;

int thread_num;
atomic_int txn_num(0);
string type[10];
pair<int,int> row[10][50000];
int row_num = 0;
int row_tot[10];
int rv[10];
mutex rv_lock;
thread td[10];
struct timespec st;


void run(int tid) { // task thread
	bool row_tmp[10] = {false};
	pair<int,int> row_tmp_val[10] = {make_pair(0,0)};
	int _rv[50000] = {0};
	ifstream fin;
	ofstream fout;
	fin.open(("thread_"s + std::to_string(tid) + ".txt"s).c_str(), ios::in);
	fout.open(("output_thread_"s + std::to_string(tid) + ".csv"s).c_str(), ios::out);
	fout << "transaction_id,type,time,value" << endl;
	struct timespec now;
	string command, name, _name, op; int txn_id, val;
	while (fin >> command >> txn_id) {
		if (command != "BEGIN"s) break;
		rv_lock.lock(); // fetch timestamp
		int timestamp = ++txn_num;
		for (uint8_t i = 1; i <= thread_num; i++) if (rv[i]) _rv[rv[i]] = timestamp;
		rv[tid] = timestamp;
		clock_gettime(CLOCK_REALTIME, &now);
		fout << txn_id << ",BEGIN," << cost << "," << endl;
		rv_lock.unlock();
		fin >> command;
		while (command != "COMMIT"s) {
			if (command == "READ"s) {
				fin >> name;
				int row_id = 0; while (type[row_id] != name) row_id++;
				if (row_tmp[row_id]) {
					val = row_tmp_val[row_id].first;
				} else {
					int idx = row_tot[row_id];
					while (true) {
						idx -= 1;
						if (row[row_id][idx].second > timestamp) continue;
						if (_rv[row[row_id][idx].second] == timestamp) continue;
						break;
					}
					val = row[row_id][idx].first;
				}
				
				clock_gettime(CLOCK_REALTIME, &now);
				fout << txn_id << "," << name << "," << cost << "," << val << endl;
			}
			if (command == "SET"s) {
				fin >> _name >> name >> op >> val; _name = _name.substr(0,_name.length()-1); 
				int row_id = 0; while (type[row_id] != name) row_id++;
				if (row_tmp[row_id]) {
					row_tmp_val[row_id].first += val * (op == "+"s ? +1 : -1);
				} else {
					int idx = row_tot[row_id];
					while (true) {
						idx -= 1;
						if (row[row_id][idx].second > timestamp) continue;
						if (_rv[row[row_id][idx].second] == timestamp) continue;
						break;
					}
					row_tmp_val[row_id] = make_pair(row[row_id][idx].first + val * (op == "+"s ? +1 : -1), timestamp);
					row_tmp[row_id] = true;
				}
			}
			fin >> command;
		}
		rv_lock.lock(); // commit data
		rv[tid] = 0;
		for (uint8_t i = 0; i < row_num; i++) if (row_tmp[i]) {
			row[i][row_tot[i]] = row_tmp_val[i];
			row_tot[i] += 1;
			row_tmp[i] = false;
		}
		clock_gettime(CLOCK_REALTIME, &now);
		fout << txn_id << ",END," << cost << "," << endl;
		rv_lock.unlock();
		fin >> val;
	}
	fin.close();
}

inline void prepare() { // data prepare
	ifstream pstream;
	pstream.open("data_prepare.txt", ios::in);
	string command, name; int val;
	while (pstream >> command >> name >> val) {
		row_tot[row_num] = 0;
		row[row_num][row_tot[row_num]++] = make_pair(val, 0);
		type[row_num++] = name;
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