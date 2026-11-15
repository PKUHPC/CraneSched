/*****************************************************************************\
 *  pmi.h - Public PMI-1 interface for Crane's native client.
 *  API declarations based on Slurm contribs/pmi/pmi.h.
 *  See http://www-unix.mcs.anl.gov/mpi/mpich2/
 *
 *  NOTE: Dynamic Process Management functions (PMI part 2) are not supported
 *  at this time. Functions required for MPI-1 (PMI part 1) are supported.
 *****************************************************************************
 *  COPYRIGHT
 *
 *  The following is a notice of limited availability of the code, and
 *  disclaimer which must be included in the prologue of the code and in all
 *  source listings of the code.
 *
 *  Copyright Notice + 2002 University of Chicago
 *
 *  Permission is hereby granted to use, reproduce, prepare derivative
 *  works, and to redistribute to others. This software was authored by:
 *
 *  Argonne National Laboratory Group
 *  W. Gropp: (630) 252-4318; FAX: (630) 252-5986; e-mail: gropp@mcs.anl.gov
 *  E. Lusk: (630) 252-7852; FAX: (630) 252-5986; e-mail: lusk@mcs.anl.gov
 *  Mathematics and Computer Science Division Argonne National Laboratory,
 *  Argonne IL 60439
 *
 *  GOVERNMENT LICENSE
 *
 *  Portions of this material resulted from work developed under a U.S.
 *  Government Contract and are subject to the following license: the
 *  Government is granted for itself and others acting on its behalf a
 *  paid-up, nonexclusive, irrevocable worldwide license in this computer
 *  software to reproduce, prepare derivative works, and perform publicly
 *  and display publicly.
 *
 *  DISCLAIMER
 *
 *  This computer code material was prepared, in part, as an account of work
 *  sponsored by an agency of the United States Government. Neither the
 *  United States, nor the University of Chicago, nor any of their
 *  employees, makes any warranty express or implied, or assumes any legal
 *  liability or responsibility for the accuracy, completeness, or
 *  usefulness of any information, apparatus, product, or process disclosed,
 *  or represents that its use would not infringe privately owned rights.
 *
 *  MCS Division <http://www.mcs.anl.gov>        Argonne National Laboratory
 *  <http://www.anl.gov>     University of Chicago <http://www.uchicago.edu>
\*****************************************************************************/

#ifndef CRANE_PMI_H_INCLUDED
#define CRANE_PMI_H_INCLUDED

#define PMI_SUCCESS 0
#define PMI_FAIL (-1)
#define PMI_ERR_INIT 1
#define PMI_ERR_NOMEM 2
#define PMI_ERR_INVALID_ARG 3
#define PMI_ERR_INVALID_KEY 4
#define PMI_ERR_INVALID_KEY_LENGTH 5
#define PMI_ERR_INVALID_VAL 6
#define PMI_ERR_INVALID_VAL_LENGTH 7
#define PMI_ERR_INVALID_LENGTH 8
#define PMI_ERR_INVALID_NUM_ARGS 9
#define PMI_ERR_INVALID_ARGS 10
#define PMI_ERR_INVALID_NUM_PARSED 11
#define PMI_ERR_INVALID_KEYVALP 12
#define PMI_ERR_INVALID_SIZE 13
#define PMI_ERR_INVALID_KVS 14
#define PMI_ERR_NOT_SUPPORTED PMI_FAIL
#define PMI_TRUE 1
#define PMI_FALSE 0
typedef int PMI_BOOL;
typedef struct PMI_keyval_t {
  const char* key;
  const char* val;
} PMI_keyval_t;

#ifdef __cplusplus
extern "C" {
#endif

int PMI_Init(int* spawned);
int PMI_Initialized(PMI_BOOL* initialized);
int PMI_Finalize(void);
int PMI_Abort(int exit_code, const char message[]);
int PMI_Get_size(int* size);
int PMI_Get_rank(int* rank);
int PMI_Get_universe_size(int* size);
int PMI_Get_appnum(int* appnum);
int PMI_Get_clique_size(int* size);
int PMI_Get_clique_ranks(int ranks[], int length);
int PMI_Get_id(char id[], int length);
int PMI_Get_kvs_domain_id(char id[], int length);
int PMI_Get_id_length_max(int* length);
int PMI_Barrier(void);
int PMI_KVS_Get_my_name(char name[], int length);
int PMI_KVS_Get_name_length_max(int* length);
int PMI_KVS_Get_key_length_max(int* length);
int PMI_KVS_Get_value_length_max(int* length);
int PMI_KVS_Put(const char name[], const char key[], const char value[]);
int PMI_KVS_Commit(const char name[]);
int PMI_KVS_Get(const char name[], const char key[], char value[], int length);
int PMI_KVS_Create(char name[], int length);
int PMI_KVS_Destroy(const char name[]);
int PMI_Publish_name(const char service[], const char port[]);
int PMI_Unpublish_name(const char service[]);
int PMI_Lookup_name(const char service[], char port[]);
int PMI_Spawn_multiple(int count, const char* commands[], const char** argvs[],
                       const int maxprocs[], const int info_sizes[],
                       const PMI_keyval_t* info[], int preput_size,
                       const PMI_keyval_t preput[], int errors[]);
#ifdef __cplusplus
}
#endif
#endif
