// CRACIUN FLAVIA - MARIA
// 336CA

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include "topology.h"
#include "array_calc.h"

int main(int argc, char *argv[]){
    int n_procs, rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &n_procs); // Total number of processes.
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);    // The current process ID / Rank.

    int dim_vec = atoi(argv[1]);
    int err_com = atoi(argv[2]);

    int **topology = malloc(sizeof(int *) * (MAX_CLUSTERS + 1));
    for (int i = 0; i < MAX_CLUSTERS; i++){
        topology[i] = calloc(sizeof(int), n_procs + 1);
    }

    if (err_com == 0 || err_com == 1){
        if (rank < MAX_CLUSTERS){
            // Find the topology.
            find_topology(rank, &topology, n_procs);
        }
        // Share the topology with all clusters.
        MPI_Barrier(MPI_COMM_WORLD);
        share_topology(rank, &topology, n_procs);

        int *vect = calloc(dim_vec + 1, sizeof(int));
        // Distribution of calculations.
        MPI_Barrier(MPI_COMM_WORLD);
        calculate(&vect, rank, topology, dim_vec, n_procs);

        // Calculate what's left of the array.
        MPI_Barrier(MPI_COMM_WORLD);
        last_round(rank, &vect, dim_vec, n_procs, topology);

        free(vect);
    }

    for (int i = 0; i < MAX_CLUSTERS; i++){
        free(topology[i]);
    }
    free(topology);

    MPI_Finalize();
}
