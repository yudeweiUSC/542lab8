#include <bits/stdc++.h>
#include <math.h>
#include <sys/stat.h>
#include <string>

#include <mpi.h>

using namespace std;
using std::cout;

constexpr long long int MEMORY = 1083741824;  // 1 GiB
size_t lastPos = 0;
long long int buff_size = 1048576;  // 1 MiB

struct pairs {  // some_word: some_count
  char word[30];
  size_t count;
};

char* readFile(FILE* file, size_t fileSize) {
  long long int readsize = min((size_t)buff_size, fileSize - lastPos);
  if (readsize <= 0)
    return nullptr;

  char* str = new char[readsize + 1];
  long start = 0;
  fseek(file, lastPos, SEEK_SET);
  fread(str, 1, readsize, file);

  if (readsize > 50)
    start = readsize - 50;
  char* currptr = strchr(&str[start], ' ');
  char* pre = nullptr;
  if (readsize < buff_size || currptr == nullptr) {
    lastPos = fileSize;
    str[readsize] = '\0';
    return str;
  }

  while (currptr != nullptr) {
    pre = currptr;
    currptr = strchr(currptr + 1, ' ');
  }

  int rSize = pre - str + 1;
  if (pre != nullptr)
    *pre = '\0';
  lastPos += rSize;
  return str;
}

int main(int argc, char** argv) {
  int nTotalLines = 0, blocks[2] = {1, 30};
  MPI_Aint charex, intex, displacements[2];
  MPI_Datatype obj_type, types[2] = {MPI_INT, MPI_CHAR};
  int max_count = 1, min_count = 1;
  buff_size = MEMORY;
  double startTime = 0;
  int nTasks, rank;
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nTasks);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Type_extent(MPI_INT, &intex);
  MPI_Type_extent(MPI_CHAR, &charex);
  displacements[0] = (MPI_Aint)(0);
  displacements[1] = intex;
  MPI_Type_struct(2, blocks, displacements, types, &obj_type);
  MPI_Type_commit(&obj_type);

  char* fileBuf;
  long long int* StartId;
  startTime = MPI_Wtime();

  // read the file at master node
  size_t fileSize;
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
    fileSize = filestatus.st_size;
  }

  // map for saving the String and frequency!

  double totalTime_noRead = 0, totalTime_NoDist = 0;
  map<string, int> totalHashMap;  // string for word string and int for frequency of the word

  while (true) {
    int status = 1;
    if (rank == 0) {
      StartId = new long long int[buff_size / 10];
      fileBuf = readFile(file, fileSize);
      StartId[0] = 0;
      if (fileBuf == NULL) {
        status = 0;
      }
    }
    nTotalLines = 0;
    MPI_Bcast(&status, 1, MPI_INT, 0, MPI_COMM_WORLD);
    cout << "send status from rank 0" << endl;
    if (status == 0) {
      break;
    }
    // Mapping operation: all other process gets the data through parent process 0
    if (rank == 0) {
      char* currptr = strchr(fileBuf, 10);
      while (!(currptr == NULL)) {
        StartId[++nTotalLines] = 1 + currptr - fileBuf;
        currptr = strchr(currptr + 1, 10);
      }
      StartId[nTotalLines + 1] = strlen(fileBuf);
    }
    double startTime_noRead = MPI_Wtime();

    char* buffer = NULL;
    int totalChars = 0, chunks = 0, startNum = 0, endNum = 0;

    if (rank == 0) {
      startNum = 0;
      chunks = nTotalLines / nTasks;
      endNum = chunks;
      buffer = new char[StartId[endNum] + 1];
      strncpy(buffer, fileBuf, StartId[endNum]);
      buffer[StartId[endNum]] = '\0';
      for (int i = 1; i <= nTasks - 1; ++i) {
        int curStartNum = chunks * i;
        int curEndNum = chunks * (i + 1) - 1;
        if (i + 1 == nTasks) {
          curEndNum = nTotalLines;
        }
        if (curStartNum < 0) {
          curStartNum = 0;
        }
        int curLength = StartId[curEndNum + 1] - StartId[curStartNum];
        MPI_Send(&curLength, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
        if (curLength > 0) {
          MPI_Send(fileBuf + StartId[curStartNum], curLength, MPI_CHAR, i, 2, MPI_COMM_WORLD);
        }
      }
      cout << "send message from rank 0" << endl;
      free(fileBuf);
      free(StartId);
    } else {
      MPI_Status status;
      MPI_Recv(&totalChars, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, &status);  // receive data to process
      if (totalChars > 0) {
        buffer = new char[totalChars + 1];
        MPI_Recv(buffer, totalChars, MPI_CHAR, 0, 2, MPI_COMM_WORLD, &status);
        buffer[totalChars] = '\0';
      }
      cout << "message received from rank 0 at rank " << rank << endl;
    }

    pairs* words = nullptr;
    int mapSize = 0;
    double startTime_noDist = MPI_Wtime();
    map<string, int> HashMap;

    if (buffer != nullptr) {
      char* word = strtok(buffer,
                          " ,:;/{})-(|][?"
                          "''!&%$#@.\r\n\t\0");
      while (word != nullptr) {
        if (HashMap.find(word) != HashMap.end()) {
          HashMap[word]++;
        } else {
          HashMap[word] = 1;
        }
        word = strtok(NULL,
                      " ,:;/{})-(|][?"
                      "''!&%$#@.\r\n\t\0");
      }
      free(buffer);

      mapSize = HashMap.size();

      if (mapSize > 0) {  // copy from map to c data structure
        words = (pairs*)malloc(mapSize * sizeof(pairs));
        int i = 0;
        for (map<string, int>::iterator it = HashMap.begin(); it != HashMap.end(); it++) {
          strcpy(words[i].word, (it->first).c_str());
          words[i].count = it->second;
          i++;
        }
      }
    }

    cout << "HashMap ready in rank " << rank << endl;

    if (rank == 0) {
      for (int i = 1; i < nTasks; i++) {
        int leng;
        MPI_Status status;
        MPI_Recv(&leng, 1, MPI_INT, i, 3, MPI_COMM_WORLD, &status);

        if (leng > 0) {
          pairs* local_words = (pairs*)malloc(leng * sizeof(pairs));
          MPI_Recv(local_words, leng, obj_type, i, 4, MPI_COMM_WORLD, &status);

          for (int j = 0; j < leng; j++) {
            if (totalHashMap.find(local_words[j].word) != totalHashMap.end()) {
              totalHashMap[local_words[j].word] += local_words[j].count;
            } else {
              totalHashMap[local_words[j].word] = local_words[j].count;
            }
          }
          free(local_words);
        }
      }
      cout << "HashMap ready in rank 0" << endl;
    } else {
      MPI_Send(&mapSize, 1, MPI_INT, 0, 3, MPI_COMM_WORLD);
      if (mapSize > 0) {
        MPI_Send(words, mapSize, obj_type, 0, 4, MPI_COMM_WORLD);
      }
    }
    HashMap.clear();
    if (words != NULL && mapSize > 0) {
      delete[] words;
    }
    totalTime_noRead += (MPI_Wtime() - startTime_noRead);
    totalTime_NoDist += (MPI_Wtime() - startTime_noDist);
  }

  // print the result
  if (rank == 0) {
    fclose(file);
    double t = MPI_Wtime();

    for (const auto& key_value : totalHashMap)
      if (key_value.second == max_count)
        cout << "Min word count" << key_value.first << ": " << key_value.second << endl;

    for (const auto& key_value : totalHashMap)
      if (key_value.second == min_count)
        cout << "Min word count" << key_value.first << ": " << key_value.second << endl;

    double endTime = MPI::Wtime();
    cout << "Total Time taken: " << totalTime_NoDist + endTime - t << " seconds" << endl;
    totalHashMap.clear();
  }

  MPI_Finalize();
  return 0;
}
