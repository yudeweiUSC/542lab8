#include <bits/stdc++.h>
#include <math.h>
#include <sys/stat.h>
#include <string>
#include <map>
#include <iostream>

#include <mpi.h>

using namespace std;

constexpr long long int KMemory = 1083741824;  // 1 GiB
size_t last_pos = 0;
long long int buff_size = 0;  // 1 MiB

struct pairs {  // some_character: some_count
  char character;
  size_t count;
};

char* readFile(FILE* file, size_t file_size) {
  long long int read_size = min((size_t)buff_size, file_size - last_pos);
  if (read_size <= 0)
    return nullptr;

  char* str = new char[read_size + 1];
  long start = 0;
  fseek(file, last_pos, SEEK_SET);
  fread(str, 1, read_size, file);

  if (read_size > 50)
    start = read_size - 50;
  char* currptr = strchr(&str[start], ' ');
  char* pre = nullptr;
  if (read_size < buff_size || currptr == nullptr) {
    last_pos = file_size;
    str[read_size] = '\0';
    return str;
  }

  while (currptr != nullptr) {
    pre = currptr;
    currptr = strchr(currptr + 1, ' ');
  }

  int r_size = pre - str + 1;
  if (pre != nullptr)
    *pre = '\0';
  last_pos += r_size;
  return str;
}

int main(int argc, char** argv) {
  int n_total_lines = 0, blocks[2] = {1, 1};
  MPI_Aint charex, intex, displacements[2];
  MPI_Datatype obj_type, types[2] = {MPI_INT, MPI_CHAR};
  int max_count = 1, min_count = 1;
  buff_size = KMemory;
  double start_time = 0;
  int count_task, rank;
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &count_task);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Type_extent(MPI_INT, &intex);
  MPI_Type_extent(MPI_CHAR, &charex);
  displacements[0] = (MPI_Aint)(0);
  displacements[1] = intex;
  MPI_Type_struct(2, blocks, displacements, types, &obj_type);
  MPI_Type_commit(&obj_type);

  constexpr int KMapInfoTag = 1;
  constexpr int KMapDataTag = 2;
  constexpr int KReduceInfoTag = 3;
  constexpr int KReduceDataTag = 4;

  char* file_buf;
  long long int* start_id;
  start_time = MPI_Wtime();

  std::cout << "----------------------------------------------------------" << std::endl;
  // read the file at master node
  size_t file_size;
  FILE* file;
  if (rank == 0) {
    // open the file
    if (argc >= 2) {
      file = fopen(argv[1], "r+");
    } else {
      file = fopen("gutenberg-1G.txt", "r+");
    }
    struct stat filestatus;
    if (argc >= 2) {
      stat(argv[1], &filestatus);
    } else {
      stat("gutenberg-1G.txt", &filestatus);
    }
    file_size = filestatus.st_size;
  }

  double totalTime_noRead = 0, totalTime_NoDist = 0;
  map<char, int> global_map;

  while (true) {
    int status = 1;
    if (rank == 0) {
      start_id = new long long int[buff_size / 10];
      file_buf = readFile(file, file_size);
      start_id[0] = 0;
      if (file_buf == NULL) {
        status = 0;
      }
    }
    n_total_lines = 0;
    MPI_Bcast(&status, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (status == 0) {
      break;
    }

    //Mapping operation: all other process gets the data through parent process 0

    if (rank == 0) {
      char* currptr = strchr(file_buf, 10);
      while (!(currptr == NULL)) {
        start_id[++n_total_lines] = 1 + currptr - file_buf;
        currptr = strchr(currptr + 1, 10);
      }
      start_id[n_total_lines + 1] = strlen(file_buf);
    }

    double start_time_noRead = MPI_Wtime();

    char* buffer = NULL;
    int total_chars = 0, portion = 0, startNum = 0, endNum = 0;

// Map ----------------------------------------------------------

    if (rank == 0) {
      startNum = 0;
      portion = n_total_lines / count_task;
      endNum = portion;
      buffer = new char[start_id[endNum] + 1];
      strncpy(buffer, file_buf, start_id[endNum]);
      buffer[start_id[endNum]] = '\0';
      for (int i = 1; i <= count_task - 1; i++) {
        int curStartNum = portion * i;
        int curEndNum = portion * (i + 1) - 1;
        if (i + 1 == count_task) {
          curEndNum = n_total_lines;
        }
        if (curStartNum < 0) {
          curStartNum = 0;
        }
        int curLength = start_id[curEndNum + 1] - start_id[curStartNum];
        MPI_Send(&curLength, 1, MPI_INT, i, KMapInfoTag, MPI_COMM_WORLD);
        if (curLength > 0) {
          MPI_Send(file_buf + start_id[curStartNum], curLength, MPI_CHAR, i, KMapDataTag, MPI_COMM_WORLD);
        }
      }
      std::cout << "Map:\trank " << rank << " has sent data"  << std::endl;
      free(file_buf);
      free(start_id);
    } else {
      MPI_Status status;
      MPI_Recv(&total_chars, 1, MPI_INT, 0, KMapInfoTag, MPI_COMM_WORLD, &status);  // receive data to process
      if (total_chars > 0) {
        buffer = new char[total_chars + 1];
        MPI_Recv(buffer, total_chars, MPI_CHAR, 0, KMapDataTag, MPI_COMM_WORLD, &status);
        buffer[total_chars] = '\0';
      }
      std::cout << "Map:\trank " << rank << " has received data"  << std::endl;
    }

// Process ----------------------------------------------------------

    pairs* words = nullptr;
    int mapSize = 0;
    double start_time_noDist = MPI_Wtime();
    map<char, int> local_map;

    if (buffer != nullptr) {
      char* word = strtok(buffer, " \r\n\t");
      while (word != NULL) {
        char NewCharacter;
        for (int j = 0; j < strlen(word); j++) {
          NewCharacter = word[j];
          if (local_map.find(NewCharacter) != local_map.end()) {
            local_map[NewCharacter]++;
          } else {
            local_map[NewCharacter] = 1;
          }
        }
        word = strtok(NULL, " \r\n\t");
      }
      delete [] buffer;
      mapSize = local_map.size();

      if (mapSize > 0) {
        words = (pairs*)malloc(mapSize * sizeof(pairs));
        int i = 0;
        for (const auto& key_value: local_map) {
          words[i].character = key_value.first;
          words[i].count = key_value.second;
          i++;
        }
      }
    }

    std::cout << "Process:rank " << rank << " has processed the local_map with size " << local_map.size() << std::endl;

// Reduce ----------------------------------------------------------

    if (rank == 0) {
      for (int i = 1; i < count_task; i++) {
        int leng;
        MPI_Status status;
        MPI_Recv(&leng, 1, MPI_INT, i, KReduceInfoTag, MPI_COMM_WORLD, &status);

        if (leng > 0) {
          pairs* local_words = (pairs*)malloc(leng * sizeof(pairs));
          MPI_Recv(local_words, leng, obj_type, i, KReduceDataTag, MPI_COMM_WORLD, &status);

          for (int j = 0; j < leng; j++) {
            if (global_map.find(local_words[j].character) != global_map.end()) {
              global_map[local_words[j].character] += local_words[j].count;
            } else {
              global_map[local_words[j].character] = local_words[j].count;
            }
          }

          
          free(local_words);
        }
      }

      for (const auto& it: local_map) {
        global_map[it.first] += it.second;
        if (global_map[it.first] > max_count) {
          max_count = global_map[it.first];
        }
        if (global_map[it.first] < min_count) {
          min_count = global_map[it.first];
        }
      }

      std::cout << "Reduce:\trank " << rank << " has received data"  << std::endl;

    } else {
      MPI_Send(&mapSize, 1, MPI_INT, 0, KReduceInfoTag, MPI_COMM_WORLD);
      if (mapSize > 0) {
        MPI_Send(words, mapSize, obj_type, 0, KReduceDataTag, MPI_COMM_WORLD);
      }
      std::cout << "Reduce:\trank " << rank << " has sent data"  << std::endl;
    }

    local_map.clear();
    if (words != NULL && mapSize > 0) {
      delete[] words;
    }
    totalTime_noRead += (MPI_Wtime() - start_time_noRead);
    totalTime_NoDist += (MPI_Wtime() - start_time_noDist);
  }

// Output ----------------------------------------------------------
  if (rank == 0) {
    fclose(file);
    double t = MPI_Wtime();

    std::cout << "----------------------------------------------------------" << std::endl;

    for (const auto& key_value : global_map)
      std::cout << "character " << key_value.first << ": " << key_value.second << std::endl;

    std::cout << "----------------------------------------------------------" << std::endl;
    for (const auto& key_value : global_map)
      if (key_value.second == max_count)
        std::cout << "Max character count " << key_value.first << ": " << key_value.second << std::endl;

    for (const auto& key_value : global_map)
      if (key_value.second == min_count)
        std::cout << "Min character count " << key_value.first << ": " << key_value.second << std::endl;

    std::cout << "----------------------------------------------------------" << std::endl;
    double endTime = MPI::Wtime();
    std::cout << "Total Time taken: " << totalTime_NoDist + endTime - t << " seconds" << std::endl;
    global_map.clear();

    std::cout << "----------------------------------------------------------" << std::endl;
  }

  MPI_Finalize();
  return 0;
}