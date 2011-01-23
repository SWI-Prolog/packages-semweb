/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2011, VU University Amsterdam

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef RDF_QUERY_H_INCLUDED
#define RDF_QUERY_H_INCLUDED


		 /*******************************
		 *	    GENERATIONS		*
		 *******************************/

typedef uint64_t gen_t;			/* Basic type for generations */

typedef struct lifespan
{ gen_t		birth;			/* Generation we were born */
  gen_t		death;			/* Generation we died */
} lifespan;

#define GEN_MAX		0x7fffffffffffffff /* Max `normal' generation */
#define GEN_TBASE	0x8000000000000000 /* Transaction generation base */
#define GEN_TNEST	0x0000000100000000 /* Max transaction nesting */

					/* Generation of a transaction */
#define T_GEN(tid,d)	(GEN_TBASE + (tid)*GEN_TNEST + (d))


		 /*******************************
		 *	      WAITERS		*
		 *******************************/

#include <semaphore.h>

typedef struct wait_on_queries
{ int	active_count;			/* #Running queries */
  void (*ondied)(DB *db, void *closure); /* Call-back */
} wait_on_queries;


typedef struct wait_list
{ wait_on_queries   waiter;		/* Waiting structure */
  struct wait_list *next;		/* Next waiting */
} wait_list;


		 /*******************************
		 *	      THREADS		*
		 *******************************/

typedef struct thread_info
{ struct query *transaction;		/* Current transaction */
} thread_info;

#define MAX_BLOCKS 20			/* allows for 2M threads */

typedef struct per_thread
{ thread_info blocks[MAX_BLOCKS];
  thread_info preallocated[7];
} per_thread;

		 /*******************************
		 *	      QUERIES		*
		 *******************************/

typedef enum q_type
{ Q_NORMAL = 0,				/* Normal query */
  Q_TRANSACTION				/* A transaction */
} q_type;


typedef struct query
{ gen_t		generation;		/* Generation that started the Q */
  rdf_db       *db;			/* Database on which we run */
  int		thread;			/* Prolog thread-id running the Q */
  thread_info  *thread_info;		/* Per-thread administration */
  q_type	type;			/* Q_* */
  wait_list    *waiters;		/* things waiting for me to die */
  struct
  { gen_t	generation;		/* generation of the transaction */
    struct query *parent;		/* Parent transaction */
  } trans;
} query;


typedef struct query_admin
{ gen_t		generation;		/* Global heart-beat */
  per_thread	per_thread;		/* per-thread data (transactions) */
  query	       *queries;		/* Open queries */
  struct
  { mutex_t	add;			/* For adding triples */
  } locks;
} query_admin.


		 /*******************************
		 *	    	API		*
		 *******************************/

COMMON(query *)		alloc_query(rdf_db *db);
COMMON(query *)		alloc_transaction(rdf_db *db);
COMMON(query *)		free_query(query *q);

COMMON(gen_t)		oldest_query(rdf_db *db,
				     void (*ondied)(rdf_db *db, void *closure));

					/* Inline? */
COMMON(int)		alive(query *q, lifespan *span);




#endif /*RDF_QUERY_H_INCLUDED*/
