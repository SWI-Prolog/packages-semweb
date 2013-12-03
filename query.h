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
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
    MA 02110-1301 USA
*/

#ifndef RDF_QUERY_H_INCLUDED
#define RDF_QUERY_H_INCLUDED
#include <stdint.h>
#include "rdf_db.h"
#include "mutex.h"




typedef struct rdf_db *rdf_dbp;
typedef struct triple *triplep;


		 /*******************************
		 *     TRANSITIVE RELATIONS	*
		 *******************************/

typedef struct visited
{ struct visited *next;			/* next in list */
  struct visited *hash_link;		/* next in hashed link */
  atom_t resource;			/* visited resource */
  uintptr_t distance;			/* Distance */
} visited;

typedef struct agenda
{ struct query *query;			/* associated query */
  visited *head;			/* visited list */
  visited *tail;			/* tail of visited list */
  visited *to_expand;			/* next to expand */
  visited *to_return;			/* next to return */
  visited **hash;			/* hash-table for cycle detection */
  int	  hash_size;
  int     size;				/* size of the agenda */
  uintptr_t max_d;			/* max distance */
  triple  pattern;			/* partial triple used as pattern */
  atom_t  target;			/* resource we are seaching for */
  struct chunk  *chunk;			/* node-allocation chunks */
} agenda;

#ifndef offsetof
#define offsetof(structure, field) ((size_t) &(((structure *)NULL)->field))
#endif
#define CHUNK_SIZE(n) offsetof(chunk, nodes[n])

typedef struct chunk
{ struct chunk *next;
  int	 used;				/* # used elements */
  int	 size;				/* size of the chunk */
  struct visited nodes[1];		/* nodes in the chunk */
} chunk;


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
  gen_t		tr_gen;			/* generation for transaction reading */
  gen_t		reindex_gen;		/* Reindex counter at start */
  rdf_dbp	db;			/* Database on which we run */
  struct query *parent;			/* Parent query */
  struct query_stack  *stack;		/* Query-stack I am part of */
  q_type	type;			/* Q_* */
  int		depth;			/* recursion depth */
  struct query *transaction;		/* Transaction of the query */
  struct
  { struct triple_buffer *added;
    struct triple_buffer *deleted;
    struct triple_buffer *updated;
    term_t	prolog_id;		/* Prolog transaction identifier */
    list	lifespans;		/* Lifespans that must be invalidated */
  } transaction_data;
  union query_state
  { search_state	search;		/* State for normal searches */
    agenda		tr_search;	/* State for transitive searches */
    struct
    { int	 prop;
      predicate *pred;
    } predprop;
  } state;
} query;

#define MAX_QBLOCKS 20			/* allows for 2M concurrent queries */

typedef struct query_stack
{ query	       *blocks[MAX_QBLOCKS];
  query		preallocated[4];
  simpleMutex	lock;
  query	       *transaction;		/* Current transaction */
  gen_t		tr_gen_base;		/* Base generation for transactions */
  gen_t		tr_gen_max;		/* Max generation for transactions */
  rdf_dbp	db;			/* DB we are associated to */
  int		top;			/* Top of query stack */
} query_stack;


		 /*******************************
		 *	      THREADS		*
		 *******************************/

typedef struct thread_info
{ query_stack   queries;		/* Open queries */
} thread_info;

		 /*******************************
		 *		API		*
		 *******************************/

COMMON(void)	init_query_admin(rdf_dbp db);
COMMON(query *)	open_query(rdf_dbp db);
COMMON(void)	close_query(query *q);
COMMON(gen_t)	oldest_query_geneneration(rdf_db *db, gen_t *reindex_gen);

COMMON(query *)	open_transaction(rdf_dbp db,
				 struct triple_buffer *added,
				 struct triple_buffer *deleted,
				 struct triple_buffer *updated,
				 snapshot *ss);
COMMON(int)	empty_transaction(query *q);
COMMON(int)	commit_transaction(query *q);
COMMON(void)	close_transaction(query *q);
COMMON(int)	discard_transaction(query *q);

COMMON(int)	add_triples(query *q, triplep *triples, size_t count);
COMMON(int)	del_triples(query *q, triplep *triples, size_t count);
COMMON(int)	update_triples(query *q,
			       triplep *old, triplep *new, size_t count);
COMMON(int)	alive_lifespan(query *q, lifespan *span);
COMMON(int)	born_lifespan(query *q, lifespan *lifespan);
COMMON(char *)	gen_name(gen_t gen, char *buf);

#ifdef COMPACT
static triple *
fetch_triple(rdf_db *db, triple_id id)
{ return id ? db->triple_array.blocks[MSB(id)][id].triple : (triple*)NULL;
}
#else /*COMPACT*/
#define fetch_triple(db, t) (t)
#endif /*COMPACT*/

/* dereference `optimized' triples.  See optimize_triple_hash()

   FIXME: Things are generally not so easy.  See alive_triple() for
   avoiding duplicates.
*/

static inline triple *
deref_triple(rdf_db *db, triple *t)
{ while(t->reindexed)
    t = fetch_triple(db, t->reindexed);

  return t;
}

/* Find out whether a triple is alive and, if the triple is reindexed,
   return the current version.  Note that if the triple was reindexed
   before this query was started, we will find the reindexed one as
   well, so we can discard this one.
*/

static inline triple *
alive_triple(query *q, triple *t)
{ for ( ; t->reindexed; t = fetch_triple(q->db, t->reindexed) )
  { if ( t->lifespan.died < q->reindex_gen )
      return NULL;
  }

  return alive_lifespan(q, &t->lifespan) ? t : (triple*)NULL;
}


static inline int
overlap_lifespan(lifespan *l1, lifespan *l2)
{ if ( l1->died < l2->born ||		/* l1 entirely before l2 */
       l2->died < l1->born )		/* l2 entirely before l1 */
    return FALSE;

  return TRUE;
}


static inline gen_t
queryWriteGen(query *q)
{ if ( q->transaction )
    return q->transaction->wr_gen;
  else
    return q->db->queries.generation;
}

static inline void
setWriteGen(query *q, gen_t gen)
{ if ( q->transaction )
    q->transaction->wr_gen = gen;
  else
    q->db->queries.generation = gen;
}

static inline gen_t
query_max_gen(query *q)
{ if ( q->transaction )
    return q->stack->tr_gen_max;
  else
    return GEN_MAX;
}

static inline gen_t
transaction_max_gen(query *q)
{ return q->stack->tr_gen_max;
}

static inline int
is_wr_transaction_gen(query *q, gen_t gen)
{ if ( gen >= q->stack->tr_gen_base &&
       gen <= q->stack->tr_gen_max )
    return TRUE;

  return FALSE;
}

#endif /*RDF_QUERY_H_INCLUDED*/
