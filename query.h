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
#include <stdint.h>
#include "mutex.h"


		 /*******************************
		 *	    GENERATIONS		*
		 *******************************/

typedef uint64_t gen_t;			/* Basic type for generations */

typedef struct lifespan
{ gen_t		born;			/* Generation we were born */
  gen_t		died;			/* Generation we died */
} lifespan;

#define GEN_UNDEF	0xffffffffffffffff /* no defined generation */
#define GEN_MAX		0x7fffffffffffffff /* Max `normal' generation */
#define GEN_TBASE	0x8000000000000000 /* Transaction generation base */
#define GEN_TNEST	0x0000000100000000 /* Max transaction nesting */

					/* Generation of a transaction */
#define T_GEN(tid,d)	(GEN_TBASE + (tid)*GEN_TNEST + (d))


typedef struct rdf_db *rdf_dbp;

		 /*******************************
		 *	      WAITERS		*
		 *******************************/

typedef void (*onready)(rdf_dbp db, void *closure);

typedef struct wait_on_queries
{ simpleMutex	lock;			/* Protect active count */
  int		active_count;		/* #Running queries */
  rdf_dbp	db;			/* Database I'm associated to */
  void	       *data;			/* Closure data */
  onready      *onready;		/* Call-back */
} wait_on_queries;


typedef struct wait_list
{ wait_on_queries  *waiter;		/* Waiting structure */
  struct wait_list *next;		/* Next waiting */
} wait_list;


		 /*******************************
		 *	      QUERIES		*
		 *******************************/

typedef enum q_type
{ Q_NORMAL = 0,				/* Normal query */
  Q_TRANSACTION				/* A transaction */
} q_type;

typedef struct query
{ gen_t		rd_gen;			/* generation for reading */
  gen_t		wr_gen;			/* generation for writing */
  rdf_dbp	db;			/* Database on which we run */
  wait_list    *waiters;		/* things waiting for me to die */
  struct query *parent;			/* Parent query */
  struct query_stack  *stack;		/* Query-stack I am part of */
  q_type	type;			/* Q_* */
  int		depth;			/* recursion depth */
} query;

#define MAX_QBLOCKS 20			/* allows for 2M concurrent queries */

typedef struct query_stack
{ query	       *blocks[MAX_QBLOCKS];
  query		preallocated[4];
  simpleMutex	lock;
  gen_t		rd_gen;			/* generation for reading */
  gen_t		wr_gen;			/* generation for writing */
  rdf_dbp	db;			/* DB we are associated to */
  int		top;			/* Top of query stack */
} query_stack;


		 /*******************************
		 *	      THREADS		*
		 *******************************/

typedef struct thread_info
{ query_stack   queries;		/* Open queries */
} thread_info;

#define MAX_BLOCKS 20			/* allows for 2M threads */

typedef struct per_thread
{ thread_info **blocks[MAX_BLOCKS];
} per_thread;

typedef struct query_admin
{ gen_t		generation;		/* Global heart-beat */
  struct
  { simpleMutex	lock;
    per_thread	per_thread;
  } query;				/* active query administration */
  struct
  { simpleMutex	lock;
  } write;				/* write administration */
} query_admin;


		 /*******************************
		 *	    	API		*
		 *******************************/

COMMON(void)	init_query_admin(rdf_dbp db);
COMMON(query *)	open_query(rdf_dbp db);
COMMON(void)	close_query(query *q);

typedef struct triple *triplep;

COMMON(int)	add_triples(query *q, triplep *triples, size_t count);
COMMON(int)	del_triples(query *q, triplep *triples, size_t count);

#endif /*RDF_QUERY_H_INCLUDED*/
