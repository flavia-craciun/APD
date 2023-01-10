// CRACIUN FLAVIA - MARIA
// 336CA

#include <pthread.h>
#include <cstdlib>
#include <cmath>
#include <unistd.h>
#include <fstream>
#include <cstring>
#include <iostream>
#include <iterator>
#include <list>
#include <map>
#include <vector>
#include "pthread_barrier_mac.h"

#define MAX_STR 100
#define ID_MAPPER 0
#define ID_REDUCER 1

using namespace std;

struct mapper{
    int id;
    int E;
    int M;
    map<int, list<int> > numbers;
    vector <char *> files;
};

struct reducer{
    struct mapper **mappers;
    list <int> all;
    int id;
    int E;
    int M;
    int pow;
};

struct general{
    struct mapper *mp;
    struct reducer red; 
    int type;
};

pthread_barrier_t barrier;

// Functie folosita pentru a gasi daca exista o baza
// care ridicata la un anumit exponent sa rezulte intr-un nr dat
bool binarySearch(int number, int exp)
{
    int left = 2, right = sqrt(number) + 1;

    while (right - left > 1) {
        int mid = (right + left) / 2;
        if (pow(mid, exp) < number) {
            left = mid + 1;
        }
        else {
            right = mid;
        }
    }

    if (pow(left, exp) == number) {
        return true;
    } else {
        if (pow(right, exp) == number)
            return true;
        else
            return false;
    }
}

// Functie care implementeaza operatiile unui Mapper
void mapperFunc(mapper *mp) {
    int length = mp->files.size();

    for (long i = 0; i < length; i++) {
        ifstream input_file(mp->files[i]);
        int iters;
        input_file >> iters;

        for (int i = 0; i < iters; i++) {
            long number;
            input_file >> number;

            // Adauga in lista tuturor exponentilor
            if (number == 1) {
                for (int i = 2; i <= mp->E; i++) {
                    mp->numbers[i].push_back(number);
                }    
            } else {
                if (number == 0)
                    continue;

                // Cauta baza pentru fiecare exponent
                for (int i = 2; i <= mp->E; i++) {
                    if (binarySearch(number, i)) {
                        mp->numbers[i].push_back(number);
                    }
                }
            }
        }

        input_file.close();
    }
}

// Functie care implementeaza operatiile unui Reducer
void reducerFunc(struct reducer *r) {
    struct reducer red = *r;
    struct mapper *mppers = *red.mappers;
    string file = "out";

    list<int>::iterator itr;

    // Centralizeaza listele partiale
    for (int i = 0; i < red.M; i++) {
        for (itr = mppers[i].numbers[red.pow].begin(); itr != mppers[i].numbers[red.pow].end(); ++itr) {
            red.all.push_back(*itr);
        }
    }

    // Elimina duplicate
    red.all.sort();
    red.all.unique();

    // Afiseaza numarul de valoril unice din lista finala
    file.append(to_string(red.pow));
    file.append(".txt");

    ofstream myfile;
    myfile.open (file);
    myfile << red.all.size();

    myfile.close();
}

void *func(void *args) {
  	struct general *files = (struct general *) args;
    int type = files->type;
  
    if (type == ID_MAPPER) { // cazul Mapper
        mapperFunc(files->mp);
    }

    pthread_barrier_wait(&barrier);
    
    if (type == ID_REDUCER) { // cazul Reducer
        reducerFunc(&files->red);
    }
    
  	pthread_exit(NULL);
}

// Functie care intoarce marimea unui fisier
long int findSize(const char *file_name) {
    FILE* fp = fopen(file_name, "r");
  
    fseek(fp, 0L, SEEK_END);
  
    long int res = ftell(fp);
  
    fclose(fp);
  
    return res;
}

// Functie de sortare a fiserelor in ordinea
// descrescatoare a dimensiunii lor
int sortFiles( const void *str1, const void *str2 ) {
    const char *file1 = *(char**)str1;
    const char *file2 = *(char**)str2;

    return findSize(file2) - findSize(file1);
}

int main(int argc, char *argv[]) {
    int M, R;
    int nrOfFiles;

    if (argc < 4) {
        perror("Numar incomplet de argumente");
        exit(-1);
    }

    // Extrage datele din linia de comanda
    M = atoi(argv[1]);
    R = atoi(argv[2]);

    ifstream infile(argv[3]);
    if (!infile.is_open() || !infile.good()) {
        cout << "Failed to open file..";
        exit(1);
    }

    string line = "";
    getline(infile, line);
    nrOfFiles = atoi(line.c_str());

    struct mapper *mapperStruct = new mapper[M + 1];
    struct general *generalStruct = new general[R + M + 1];

    char **files = new char* [nrOfFiles + 1];
    for (int i = 0; i <= nrOfFiles; i++) {
        files[i] = new char[MAX_STR];
    }

    for (int i = 0; i < nrOfFiles; i++) {
        getline(infile, line);
        strcpy(files[i], line.c_str());
    }

    infile.close();

    // Sortam fisierele dupa dimensiune
    qsort(files, nrOfFiles, sizeof(char*), sortFiles);

    // Intercalam fisierele de dimensiuni mari cu cele de dimensiuni mici
    for (int i = 1; i < nrOfFiles / 2; i++) {
        swap(files[i], files[nrOfFiles - 1 - i]);
    }

    // Initializarea mapperior
    for (int i = 0; i < M ; i++) {
        int start = i * nrOfFiles / M;
        int stop = min((i + 1) * nrOfFiles / M, nrOfFiles);
        for (int j = start; j < stop; j++) {
            mapperStruct[i].files.push_back(files[j]);
        }
        mapperStruct[i].E = R + 1;
        mapperStruct[i].id = i;
        mapperStruct[i].M = M;
    }

    // Initializarea vectorului de Mapperi si Reduceri
    for (int i = 0; i < M + R ; i++) {
        if (i < M) {
            generalStruct[i].type = ID_MAPPER;
            generalStruct[i].mp = &mapperStruct[i];
        } else {
            generalStruct[i].type = ID_REDUCER;
            generalStruct[i].red.id= i - M;
            generalStruct[i].red.E = R + 1;
            generalStruct[i].red.pow = i - M + 2;
            generalStruct[i].red.M = M;
            generalStruct[i].red.mappers = &mapperStruct;
        }
    }

    pthread_t generalThreads[R + M];
    int r;
  	long id;
  	void *status;

    r = pthread_barrier_init(&barrier, NULL, R + M);
	if (r != 0) {
		printf("Eroare la crearea barrier \n");
		exit(-1);
	}
    
    for (id = 0; id < M + R; id++) {
        r = pthread_create(&generalThreads[id], NULL, func, (void *)&generalStruct[id]);

		if (r) {
	  		printf("Eroare la crearea thread-ului %ld\n", id);
	  		exit(-1);
		}
  	}

	for (id = 0; id < M + R; id++) {
        r = pthread_join(generalThreads[id], &status);

		if (r) {
	  		printf("Eroare la asteptarea thread-ului %ld\n", id);
	  		exit(-1);
		}
  	}

	r = pthread_barrier_destroy(&barrier);
	if (r != 0) {
		printf("Eroare la distrugerea barrier \n");
		exit(-1);
	}

    delete[] mapperStruct;
    delete[] generalStruct;

    for (int i = 0; i <= nrOfFiles; i++) {
        delete[] files[i];
    }
    delete[] files;

  	pthread_exit(NULL);
}
