// CRACIUN FLAVIA - MARIA
// 336CA

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_CLUSTERS 4
#define LEN 5
#define BUFF 20

// Read worker processes from coresponding files.
void read_procs(int rank, int ***topo, int n_procs){
    int **topology = *topo;

    char str1[BUFF] = "cluster", str3[] = ".txt";
    char str2[BUFF];
    sprintf(str2, "%d", rank);
    strcat(str1, str2);
    strcat(str1, str3);

    FILE *fp = fopen(str1, "r");
    char buff[LEN];
    fgets(buff, LEN, fp);

    int nr_workers = atoi(buff);

    for (int i = 0; i < nr_workers; i++){
        fgets(buff, LEN, fp);
        topology[rank][atoi(buff)] = 1; // Mark worker
    }

    fclose(fp);
}

// Print topology for given process.
void print_topology(int rank, int **topology, int n_procs){
    printf("%d ->", rank);
    for (int i = 0; i < MAX_CLUSTERS; i++){
        int comma = 0;
        printf(" %d:", i);
        for (int k = 0; k < n_procs; k++){
            if (topology[i][k] != 0){
                if (comma)
                    printf(",");
                printf("%d", k);
                comma = 1;
            }
        }
    }
    printf("\n");
}

// Add received topology to the existent one and
// send the new topoology to the next coordonator.
void message(int rankD, int rankS, int ***topo, int n_procs){
    int **topology = *topo;
    int **recv_topology = malloc(sizeof(int *) * (MAX_CLUSTERS + 1));

    for (int i = 0; i < MAX_CLUSTERS; i++){
        recv_topology[i] = calloc(sizeof(int), n_procs + 1);
    }

    // Receives the topology from the previous process.
    for (int i = 0; i < MAX_CLUSTERS; i++){
        MPI_Recv(recv_topology[i], n_procs, MPI_INT, rankS, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        for (int k = 0; k < n_procs; k++){
            if (topology[i][k] == 0){
                topology[i][k] = recv_topology[i][k];
            }
        }
    }

    // All coordonators except for C1 will send the topology to the next coord process.
    if (rankD != -1){
        for (int i = 0; i < MAX_CLUSTERS; i++){
            MPI_Send(topology[i], n_procs, MPI_INT, rankD, 0, MPI_COMM_WORLD);
        }
    }

    for (int i = 0; i < MAX_CLUSTERS; i++){
        free(recv_topology[i]);
    }
    free(recv_topology);
}

// Get the completed topology.
void find_topology(int rank, int ***topo, int n_procs){
    int **topology = *topo;
    int **recv_topology = malloc(sizeof(int *) * (MAX_CLUSTERS + 1));

    for (int i = 0; i < MAX_CLUSTERS; i++){
        recv_topology[i] = calloc(sizeof(int), n_procs + 1);
    }

    // First process starts the circle.
    if (rank == 0){
        // Reads its own topology.
        read_procs(rank, &topology, n_procs);

        // Communicate the current topology to the next cluster.
        printf("M(%d,%d)\n", rank, 3);;
        for (int i = 0; i < MAX_CLUSTERS; i++){
            MPI_Send(topology[i], n_procs, MPI_INT, 3, 0, MPI_COMM_WORLD);
        }
    } else {
        // Reads its own topolo
        read_procs(rank, &topology, n_procs);

        // C1 receives the final topology and will print it.
        if (rank == 1){
            message(-1, rank + 1, &topology, n_procs);
            print_topology(rank, topology, n_procs);
        } else { // Communicate the topology to the next cluster..
            printf("M(%d,%d)\n", rank, rank - 1);
;
            if (rank == 3){
                message(rank - 1, 0, &topology, n_procs);
            } else {
                message(rank - 1, rank + 1, &topology, n_procs);
            }
        }
    }

    for (int i = 0; i < MAX_CLUSTERS; i++){
        free(recv_topology[i]);
    }
    free(recv_topology);
}

// Distribute the topology to all worker processes.
void communicate_to_subprocs(int rank, int ***topo, int n_procs){
    int **topology = *topo;
    for (int i = 0; i < n_procs; i++){
        if (topology[rank][i] == 1){
            printf("M(%d,%d)\n", rank, i);
;
            for (int k = 0; k < MAX_CLUSTERS; k++){
                MPI_Send(topology[k], n_procs, MPI_INT, i, 0, MPI_COMM_WORLD);
            }
        }
    }
}

// Distribute the topology to all cluster coordonator processes.
void share_topology(int rank, int ***topo, int n_procs){
    int **topology = *topo;
    int **recv_topology = malloc(sizeof(int *) * (MAX_CLUSTERS + 1));

    for (int i = 0; i < MAX_CLUSTERS; i++){
        recv_topology[i] = calloc(sizeof(int), n_procs + 1);
    }

    // First process starts the circle.
    if (rank == 1){
        // Communicate the topology to the next cluster.
        printf("M(%d,%d)\n", rank, rank + 1);;
        for (int i = 0; i < MAX_CLUSTERS; i++){
            MPI_Send(topology[i], n_procs, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
        }
        communicate_to_subprocs(rank, &topology, n_procs);
    } else if (rank == 0) {
        // Last process close the circle.
        // Receives the topology from the previous process.
        for (int i = 0; i < MAX_CLUSTERS; i++){
            MPI_Recv(recv_topology[i], n_procs, MPI_INT, 3, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int k = 0; k < n_procs; k++){
                if (topology[i][k] == 0){
                    topology[i][k] = recv_topology[i][k];
                }
            }
        }
        print_topology(rank, topology, n_procs);
        communicate_to_subprocs(rank, &topology, n_procs);
    } else if (rank == 2) {
        // Communicate the topology to the next cluster.
        printf("M(%d,%d)\n", rank, rank + 1);;
        message(rank + 1, rank - 1, &topology, n_procs);
        print_topology(rank, topology, n_procs);
        communicate_to_subprocs(rank, &topology, n_procs);
    } else if (rank == 3) {
        // Communicate the topology to the next cluster.
        printf("M(%d,%d)\n", rank, 0);;
        message(0, rank - 1, &topology, n_procs);
        print_topology(rank, topology, n_procs);
        communicate_to_subprocs(rank, &topology, n_procs);
    } else {
        // Worker processes receive the topology and print it.
        for (int i = 0; i < MAX_CLUSTERS; i++){
            MPI_Recv(recv_topology[i], n_procs, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int k = 0; k < n_procs; k++){
                if (topology[i][k] == 0){
                    topology[i][k] = recv_topology[i][k];
                }
            }
        }
        print_topology(rank, topology, n_procs);
    }

    for (int i = 0; i < MAX_CLUSTERS; i++){
        free(recv_topology[i]);
    }
    free(recv_topology);
}