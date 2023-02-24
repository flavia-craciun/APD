// CRACIUN FLAVIA - MARIA
// 336CA

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_CLUSTERS 4

// Coord process decides which part of the array is distributed
// to each of its workers.
void share_to_workers(int rank, int start, int ratio, int **v, int **topology, int dim_vec, int n_procs, int last_round){
    int last_index;
    int *vect = *v;
    int *recv_vect = calloc(dim_vec + 1, sizeof(int));

    for (int i = 0; i < n_procs; i++){
        if (topology[rank][i] == 1){
            printf("M(%d,%d)\n", rank, i);
            MPI_Send(&rank, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(&start, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            if (start < dim_vec - 1){
                last_index = start + ratio;
            } else {
                last_index = start;
            }
            MPI_Send(&last_index, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(vect, dim_vec, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Recv(recv_vect, dim_vec, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int i = start; i < last_index; i++)
                vect[i] = recv_vect[i];
            start = last_index;
        }
    }

    if (last_round){
        if (rank == 3){
            MPI_Send(&last_index, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        } else if (rank != 0){
            MPI_Send(&last_index, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
        }
    } else {
        if (rank == 0){
            MPI_Send(&last_index, 1, MPI_INT, 3, 0, MPI_COMM_WORLD);
            MPI_Send(&ratio, 1, MPI_INT, 3, 0, MPI_COMM_WORLD);
        } else if (rank != 1){
            MPI_Send(&last_index, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD);
            MPI_Send(&ratio, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD);
        }
    }

    free(recv_vect);
}

// Receive information from the previous coord process.
void share_info(int rank, int dim_vec, int **v, int **topology, int n_procs){
    int start;
    int ratio;
    int *vect = *v;

    if (rank == 3){
        MPI_Recv(&start, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&ratio, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(vect, dim_vec, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    } else {
        MPI_Recv(&start, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&ratio, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(vect, dim_vec, MPI_INT, rank + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    share_to_workers(rank, start, ratio, &vect, topology, dim_vec, n_procs, 0);
}

// Worker process does the calculations on the array.
void worker_job(int rank, int **v, int dim_vec){
    int *vect = *v;
    int start, end;
    int cluster_rank;
    MPI_Recv(&cluster_rank, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&start, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&end, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(vect, dim_vec, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    for (int i = start; i < end; i++){
        vect[i] *= 5;
    }

    printf("M(%d,%d)\n", rank, cluster_rank);
    MPI_Send(vect, dim_vec, MPI_INT, cluster_rank, 0, MPI_COMM_WORLD);
}

// Coord processes send the modified array one to another.
void calculate(int **v, int rank, int **topology, int dim_vec, int n_procs){
    int *vect = *v;

    // First process starts the circle.
    if (rank == 0){
        int start = 0;

        // Create array
        for (int i = 0; i < dim_vec; i++){
            vect[i] = dim_vec - i - 1;
        }

        // Calculations per worker
        int workers = n_procs - MAX_CLUSTERS;
        int ratio = dim_vec / workers;

        share_to_workers(rank, start, ratio, &vect, topology, dim_vec, n_procs, 0);
        
        printf("M(%d,%d)\n", rank, 3);
        MPI_Send(vect, dim_vec, MPI_INT, 3, 0, MPI_COMM_WORLD);
    } else if (rank > 0 && rank < MAX_CLUSTERS) {
        share_info(rank, dim_vec, &vect, topology, n_procs);

        if (rank != 1){
            printf("M(%d,%d)\n", rank, rank - 1);
            MPI_Send(vect, dim_vec, MPI_INT, rank - 1, 0, MPI_COMM_WORLD);
        }
    } else {
        worker_job(rank, &vect, dim_vec);
    }
}

// The array is sent back from the last process back to the first
// and having the last elements modified.
void last_round(int rank, int **v, int dim_vec, int n_procs, int **topology){
    int *vect = *v;

    // Last process close the circle.
    if (rank == 0){
        int start;
        MPI_Recv(&start, 1, MPI_INT, 3, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(vect, dim_vec, MPI_INT, 3, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        share_to_workers(rank, start, 1, &vect, topology, dim_vec, n_procs, 1);

        printf("Rezultat: ");
        for (int i = 0; i < dim_vec; i++)
            printf("%d ", vect[i]);
    } else if (rank > 0 && rank < MAX_CLUSTERS) {
        int start;

        if (rank == 1){
            // Sends back the array and the index of the next element to be modified
            int workers = n_procs - MAX_CLUSTERS;
            start = dim_vec - dim_vec % workers;
        }

        if (rank != 1){
            MPI_Recv(&start, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(vect, dim_vec, MPI_INT, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }

        share_to_workers(rank, start, 1, &vect, topology, dim_vec, n_procs, 1);

        if (rank == 3){
            printf("M(%d,%d)\n", rank, 0);
            MPI_Send(vect, dim_vec, MPI_INT, 0, 0, MPI_COMM_WORLD);
        } else {
            printf("M(%d,%d)\n", rank, rank + 1);
            MPI_Send(vect, dim_vec, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
        }
    } else {
        worker_job(rank, &vect, dim_vec);
    }
}