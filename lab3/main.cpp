#include <iostream>
#include <thread>
#include <bits/stdc++.h>
#include <atomic>
#include <fstream>
// #include <sstream>
#include <unistd.h>
// #include <filesystem>

#define cost (now.tv_sec-st.tv_sec)*1000000000+(now.tv_nsec-st.tv_nsec)+pretime

using namespace std;

int thread_num;
atomic_int txn_num(0);
string type[10];
pair<int,int> row[10][50000];
// bool row_tmp[10][10];
// pair<int,int> row_tmp_val[10][10];
int row_num = 0;
int row_tot[10];
// mutex wr_lock;
int rv[10];
// int _rv[10][50000];
mutex rv_lock;
thread td[10];
struct timespec st;
FILE *redoLog;
bool finished[50000];
ifstream fin[10];
ofstream fout[10];
long long pretime = 0;


void run(int tid) {
	bool row_tmp[10] = {false};
	pair<int,int> row_tmp_val[10] = {make_pair(0,0)};
	int _rv[50000] = {0};
	// ofstream debug;
	// debug.open(("debug_"s + std::to_string(tid) + ".csv"s).c_str(), ios::out);
	if (pretime == 0) fout[tid] << "transaction_id,type,time,value" << endl;
	struct timespec now;
	string command, name, _name, op; int txn_id, val;
	while (fin[tid] >> command >> txn_id) {
		if (finished[txn_id]) {
			while (command != "COMMIT"s) fin[tid] >> command;
			fin[tid] >> command;
			continue;
		}
		if (command != "BEGIN"s) break;
		rv_lock.lock();
		int timestamp = ++txn_num;
		for (uint8_t i = 1; i <= thread_num; i++) if (rv[i]) _rv[rv[i]] = timestamp;
		rv[tid] = timestamp;
		clock_gettime(CLOCK_REALTIME, &now);
		fout[tid] << txn_id << ",BEGIN," << cost << "," << endl;
		rv_lock.unlock();
		fin[tid] >> command;
		while (command != "COMMIT"s) {
			if (command == "READ"s) {
				fin[tid] >> name;
				int row_id = 0; while (type[row_id] != name) row_id++;
				if (row_tmp[row_id]) {
					val = row_tmp_val[row_id].first;
				} else {
					int idx = row_tot[row_id];
					while (true) {
						idx -= 1;
						if (row[row_id][idx].second > timestamp) continue;
						if (_rv[row[row_id][idx].second] == timestamp) continue;
						// debug << _rv[row[row_id][idx].second] << "," << timestamp << "," << row[row_id][idx].second << endl;
						break;
					}
					val = row[row_id][idx].first;
				}
				
				clock_gettime(CLOCK_REALTIME, &now);
				fout[tid] << txn_id << "," << name << "," << cost << "," << val << endl;
				// debug << txn_id << "," << name << "," << cost << "," << row[row_id][idx].first << "," << row[row_id][idx].second << "," << idx << "," << row_tot[row_id] << endl;
				// fout[tid] << idx << endl;
			}
			if (command == "SET"s) {
				fin[tid] >> _name >> name >> op >> val; _name = _name.substr(0,_name.length()-1); 
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
				
				// if (get_lock == false) wr_lock.lock();
				// get_lock = true;
				// if (row_tmp[tid][row_id]) {
				// 	row_tmp_val[tid][row_id] = make_pair(row[row_id][idx].first + val * (op == "+"s ? +1 : -1), timestamp);
				// }
				
				// clock_gettime(CLOCK_REALTIME, &now);
				// debug << txn_id << "," << name << "," << cost << "," << row[row_id][idx].first << "," << row_tot[row_id] << "," << idx << "x" << timestamp << endl;
				// fout[tid] << row_tot[row_id]-1 << endl;
				// wr_lock[row_id].unlock();
			}
			fin[tid] >> command;
		}
		rv_lock.lock();
		rv[tid] = 0;
		fwrite(&txn_id, sizeof(int), 1, redoLog);
		for (uint8_t i = 0; i < row_num; i++) if (row_tmp[i]) {
			row[i][row_tot[i]] = row_tmp_val[i];
			row_tot[i] += 1;
			row_tmp[i] = false;
			fwrite(&row_tmp_val[i], sizeof(int), 1, redoLog);
		} else {
			fwrite(&row[i][row_tot[i]-1], sizeof(int), 1, redoLog);
		}
		clock_gettime(CLOCK_REALTIME, &now);
		fout[tid] << txn_id << ",END," << cost << "," << endl;
		fflush(redoLog);
		rv_lock.unlock();
		fin[tid] >> val;
	}
	fin[tid].close();
	fout[tid].close();
}

inline void prepare() {
	ifstream pstream;
	pstream.open("debug/data_prepare.txt", ios::in);
	string command, name; int val;
	while (pstream >> command >> name >> val) {
		row_tot[row_num] = 0;
		row[row_num][row_tot[row_num]++] = make_pair(val, 0);
		type[row_num++] = name;
	}
	pstream.close();
}

inline void recover() {
	FILE *pFile = fopen("debug/redo.log", "rb");
	int total_bytes, total_redo = 0, txn_id;
	if (pFile != NULL) {
		struct timespec st, ed;
		puts("Last crash detected, recovering...");
		clock_gettime(CLOCK_REALTIME, &st);

		fseek(pFile, 0, SEEK_END);
		total_bytes = ftell(pFile);
		total_redo = total_bytes / (sizeof(int) * (row_num + 1));
		fseek(pFile, 0, SEEK_SET);
		for (int i = 0; i < total_redo; i++) {
			fread(&txn_id, sizeof(int), 1, pFile);
			if (i < total_redo - 1) {
				fseek(pFile, 4 * row_num, SEEK_CUR);
			} else {
				for (int j = 0, val; j < row_num; j++) {
					fread(&val, sizeof(int), 1, pFile);
					row[j][0] = make_pair(val, 0);
				}
			}
			finished[txn_id] = true;
		}
		fclose(pFile);
		redoLog = fopen("debug/redo.log", "ab");
		fseek(redoLog, total_redo * (sizeof(int) * (row_num + 1)), SEEK_SET);

		for (uint8_t i = 1; i <= thread_num; i++) {
			fin[i].open(("debug/output_thread_"s + std::to_string(i) + ".csv"s).c_str(), ios::in);
			string line;
			getline(fin[i], line);
			int remain = fin[i].tellg(), first, second, third;
			while (getline(fin[i], line)) {
				first = line.find(',');
				second = line.find(',', first+1);
				third = line.find(',', second+1);
				if (finished[stoi(line.substr(0, first))] == false) {
					break;
				} else {
					pretime = max(pretime, stoll(line.substr(second+1, third-second-1)));
					remain = fin[i].tellg();
				}
			}
			fin[i].close();
			fin[i].open(("debug/thread_"s + std::to_string(i) + ".txt"s).c_str(), ios::in);
			fout[i].open(("debug/output_thread_"s + std::to_string(i) + ".csv"s).c_str(), ios::ate | ios::in);
			fout[i].seekp(remain);
		}
		clock_gettime(CLOCK_REALTIME, &ed);
		printf("Cost: %.3lfs\n", (ed.tv_sec-st.tv_sec)+(ed.tv_nsec-st.tv_nsec)/1000000000.0);
	} else {
		redoLog = fopen("debug/redo.log", "wb");
		for (uint8_t i = 1; i <= thread_num; i++) {
			fin[i].open(("debug/thread_"s + std::to_string(i) + ".txt"s).c_str(), ios::in);
			fout[i].open(("debug/output_thread_"s + std::to_string(i) + ".csv"s).c_str(), ios::out);
		}
	}
	
	
}

int main(int argc, const char *argv[]) {
    if (argc >= 2) {
		sscanf(argv[1], "%d", &thread_num);
	} else {
		thread_num = 1;
	}
	prepare();
	recover();
	clock_gettime(CLOCK_REALTIME, &st);
	for (uint8_t i = 1; i <= thread_num; i++) {
		td[i] = thread(run, i);
		// run(i);
	}
	for (uint8_t i = 1; i <= thread_num; i++) {
		td[i].join();
	}
	remove("debug/redo.log");
	puts("Complete!");
	return 0;
}