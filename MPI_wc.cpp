#include <bits/stdc++.h>
#include <math.h>
#include <sys/stat.h>
#include <string>
#include <map>
#include <iostream>

#include <mpi.h>

// we are using cpp, 
// by Yuzhe, Yuhao, Yude

constexpr long long int KMemory = 1 << 30;  // 1 GiB
long long int buff_size = 1 << 20;  // 1 MiB
size_t last_pos = 0;

struct pairs {  // some_word: some_count
  char word[32];     // historical reason I use int here :-) hahahaha bug solved
  int count;
};

char* ReadFile(FILE* file, size_t file_size) {
  long long int read_size = std::min((size_t)buff_size, file_size - last_pos);
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
  int count_task, rank;
  int blocks[2] = {1, 32};
  MPI_Aint charex, intex, displacements[2];
  MPI_Datatype obj_type, types[2] = {MPI_CHAR, MPI_INT};
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &count_task);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  // MPI_Type_extent(MPI_INT, &intex);
  // MPI_Type_extent(MPI_CHAR, &charex);
  // displacements[0] = (MPI_Aint)(0);
  // displacements[1] = intex;
  displacements[0] = offsetof(pairs, word);
  displacements[1] = offsetof(pairs, count);
  MPI_Type_struct(2, blocks, displacements, types, &obj_type);
  MPI_Type_commit(&obj_type);




  // MPI_Comm_size(MPI_COMM_WORLD, &count_task);
  // MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  // MPI_Type_extent(MPI_INT, &intex);
  // MPI_Type_extent(MPI_CHAR, &charex);
  // displacements[0] = (MPI_Aint)(0);
  // displacements[1] = intex;
  // MPI_Type_struct(2, blocks, displacements, types, &obj_type);
  // MPI_Type_commit(&obj_type);

  int n_total_lines = 0;
  int max_count = 1, min_count = 10000000;
  buff_size = KMemory;
  char* file_buf;
  long long int* start_id;

  constexpr int kMapInfoTag = 1;
  constexpr int kMapDataTag = 2;
  constexpr int kReduceInfoTag = 3;
  constexpr int kReduceDataTag = 4;

  double start_time = MPI_Wtime();

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

  double total_time = 0;
  std::map<std::string, int> global_map;  

  while (true) {

    int status = 1;
    if (rank == 0) {
      std::cout << "----------------------------------------------------------" << std::endl;
      start_id = new long long int[buff_size / 10];
      file_buf = ReadFile(file, file_size);
      start_id[0] = 0;
      if (file_buf == NULL) {
        status = 0;
      }
    }
    n_total_lines = 0;
    MPI_Bcast(&status, 1, MPI_INT, 0, MPI_COMM_WORLD);
    std::cout << "Broadcast: status = " << status << std::endl; 
    if (status == 0) {
      break;
    }

    if (rank == 0) {
      char* currptr = strchr(file_buf, 10);
      while (!(currptr == NULL)) {
        start_id[++n_total_lines] = 1 + currptr - file_buf;
        currptr = strchr(currptr + 1, 10);
      }
      start_id[n_total_lines + 1] = strlen(file_buf);
    }

    char* buffer = NULL;
    int total_chars = 0, portion = 0, start_num = 0, end_num = 0;

// Map ----------------------------------------------------------

    if (rank == 0) {
      start_num = 0;
      portion = n_total_lines / count_task;
      end_num = portion;
      buffer = new char[start_id[end_num] + 1];
      strncpy(buffer, file_buf, start_id[end_num]);
      buffer[start_id[end_num]] = '\0';
      for (int i = 1; i < count_task; ++i) {
        int current_start_number = portion * i;
        int current_end_number = portion * (i + 1) - 1;
        if (i + 1 == count_task) {
          current_end_number = n_total_lines;
        }
        if (current_start_number < 0) {
          current_start_number = 0;
        }
        int current_length = start_id[current_end_number + 1] - start_id[current_start_number];
        MPI_Send(&current_length, 1, MPI_INT, i, kMapInfoTag, MPI_COMM_WORLD);
        if (current_length > 0) {
          MPI_Send(file_buf + start_id[current_start_number], current_length, MPI_CHAR, i, kMapDataTag, MPI_COMM_WORLD);
        }
      }
      std::cout << "rank 0 info sent" << std::endl;
      // std::cout << "Map:\trank " << rank << " has sent data"  << std::endl;
      std::cout << "rank 0 info sent" << std::endl;
      free(file_buf);
      std::cout << "rank 0 info sent" << std::endl;
      free(start_id);
      std::cout << "rank 0 info sent" << std::endl;
    } else {
      MPI_Status status;
      MPI_Recv(&total_chars, 1, MPI_INT, 0, kMapInfoTag, MPI_COMM_WORLD, &status); 
      if (total_chars > 0) {
        buffer = new char[total_chars + 1];
        MPI_Recv(buffer, total_chars, MPI_CHAR, 0, kMapDataTag, MPI_COMM_WORLD, &status);
        buffer[total_chars] = '\0';
      }
      std::cout << "Map:\trank " << rank << " has received data"  << std::endl;
    }

// Process ----------------------------------------------------------

    pairs* words = nullptr;
    int mapSize = 0;
    double start_time_no_dist = MPI_Wtime();
    std::map<std::string, int> local_map;

    std::cout << "start Process:rank " << rank << " has processed the local_map with size " << local_map.size() << std::endl;
    if (buffer != nullptr) {
      char* word = strtok(buffer, " *,&.()_?:\'\"/;[]\\\r\n\t1234567890$+=-");
      while (word != nullptr) {
        local_map[word] += 1;
        word = strtok(NULL, " *,&.()_?:\'\"/;[]\\\r\n\t1234567890$+=-");
      }
      free(buffer);
      mapSize = local_map.size();

      if (mapSize > 0) {  // copy from map to c data structure
        words = (pairs*)malloc(mapSize * sizeof(pairs));
        int i = 0;
        for (const auto& key_value: local_map) {          
          strcpy(words[i].word, key_value.first.c_str());
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
        MPI_Recv(&leng, 1, MPI_INT, i, kReduceInfoTag, MPI_COMM_WORLD, &status);

        if (leng > 0) {
          pairs* local_words = (pairs*)malloc(leng * sizeof(pairs));
          MPI_Recv(local_words, leng, obj_type, i, kReduceDataTag, MPI_COMM_WORLD, &status);

          for (int j = 0; j < leng; ++ j) {
            global_map[local_words[j].word] += local_words[j].count;
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
      MPI_Send(&mapSize, 1, MPI_INT, 0, kReduceInfoTag, MPI_COMM_WORLD);
      if (mapSize > 0) {
        MPI_Send(words, mapSize, obj_type, 0, kReduceDataTag, MPI_COMM_WORLD);
      }
      std::cout << "Reduce:\trank " << rank << " has sent data"  << std::endl;
    }

    local_map.clear();
    if (words != nullptr && mapSize > 0) {
      delete[] words;
    }
    total_time += (MPI_Wtime() - start_time_no_dist);
  }

// Output ----------------------------------------------------------

  if (rank == 0) {
    fclose(file);

    std::cout << "----------------------------------------------------------" << std::endl;

    for (const auto& key_value : global_map)
      if (key_value.second == max_count)
        std::cout << "Max word count " << key_value.first << ": " << key_value.second << std::endl;

    std::cout << "----------------------------------------------------------" << std::endl;

    std::cout << "Min word count is 1: " << std::endl;
    for (const auto& key_value : global_map)
      if (key_value.second == min_count)
        std::cout << key_value.first << ", ";
    std::cout << std::endl;

    std::cout << "----------------------------------------------------------" << std::endl;
    std::cout << "Total Time taken: " << total_time << " seconds" << std::endl;

    std::cout << "----------------------------------------------------------" << std::endl;
  }

  MPI_Finalize();
  return 0;
}
