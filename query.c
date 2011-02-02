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

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
RDF-DB query management. This module keeps  track of running queries. We
need this for GC purposes.  In particular, we need to:

    * Find the oldest active generation.
    * Get a signal if all currently active queries have finished.

In addition to queries, this  module   performs  the  necessary logic on
generations:

    * Is an object visible in a query?
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <string.h>
#include "rdf_db.h"
#include "query.h"
#include "memory.h"
#include "mutex.h"

static void	init_query_stack(rdf_db *db, query_stack *qs);


		 /*******************************
		 *	    THREAD DATA		*
		 *******************************/

/* Return the per-thread data for the given DB.  This version uses no
   locks for the common case that the required datastructures are
   already provided.
*/

thread_info *
rdf_thread_info(rdf_db *db, int tid)
{ query_admin *qa = &db->queries;
  per_thread *td = &qa->query.per_thread;
  thread_info *ti;
  size_t idx = MSB(tid);

  if ( !td->blocks[idx] )
  { simpleMutexLock(&qa->query.lock);
    if ( !td->blocks[idx] )
    { size_t bs = (size_t)1<<idx;
      thread_info **newblock = rdf_malloc(db, bs*sizeof(thread_info*));

      memset(newblock, 0, bs*sizeof(thread_info));

      td->blocks[idx] = newblock-bs;
    }
    simpleMutexUnlock(&qa->query.lock);
  }

  if ( !(ti=td->blocks[idx][tid]) )
  { simpleMutexLock(&qa->query.lock);
    if ( !(ti=td->blocks[idx][tid]) )
    { ti = rdf_malloc(db, sizeof(*ti));
      memset(ti, 0, sizeof(*ti));
      init_query_stack(db, &ti->queries);
      MemoryBarrier();
      td->blocks[idx][tid] = ti;
    }
    simpleMutexUnlock(&qa->query.lock);
  }

  return ti;
}


		 /*******************************
		 *	       WAITER		*
		 *******************************/

wait_on_queries *
alloc_waiter(rdf_db *db, onready *onready, void *data)
{ wait_on_queries *w = rdf_malloc(db, sizeof(*w));

  simpleMutexInit(&w->lock);
  w->active_count = 0;
  w->db           = db;
  w->data         = data;
  w->onready      = onready;

  return w;
}


static void
wakeup(wait_on_queries *w)
{ int count;

  simpleMutexLock(&w->lock);
  count = --w->active_count;
  simpleMutexUnlock(&w->lock);
  if ( count == 0 )
  { simpleMutexDelete(&w->lock);
    Sdprintf("Wait complete!\n");
    if ( w->onready )
      (*w->onready)(w->db, w->data);
    rdf_free(w->db, w, sizeof(*w));
  }
}


		 /*******************************
		 *	    QUERY-STACK		*
		 *******************************/

static void
preinit_query(rdf_db *db, query_stack *qs, query *q, query *parent, int depth)
{ q->db     = db;
  q->stack  = qs;
  q->parent = q;
  q->depth  = depth;
}


static void
init_query_stack(rdf_db *db, query_stack *qs)
{ int i;
  int prealloc = sizeof(qs->preallocated)/sizeof(qs->preallocated[0]);
  query *parent = NULL;

  memset(qs, 0, sizeof(*qs));

  simpleMutexInit(&qs->lock);
  qs->db = db;
  qs->rd_gen = qs->wr_gen = GEN_UNDEF;

  for(i=0; i<MSB(prealloc); i++)
    qs->blocks[i] = qs->preallocated;
  for(i=0; i<prealloc; i++)
  { query *q = &qs->preallocated[i];

    preinit_query(db, qs, q, parent, i);
    parent = q;
  }
}


query *
alloc_query(query_stack *qs)
{ int depth = qs->top;
  int b = MSB(depth);

  if ( qs->blocks[b] )
    return &qs->blocks[b][depth];

  simpleMutexLock(&qs->lock);
  if ( !qs->blocks[b] )
  { size_t bytes = (1<<b) * sizeof(query);
    query *ql = rdf_malloc(qs->db, bytes);
    query *parent;
    int i;

    memset(ql, 0, bytes);
    ql -= depth;			/* rebase */
    parent = &qs->blocks[b-1][depth-1];
    for(i=depth; i<depth*2; i++)
    { query *q = &ql[depth];
      preinit_query(qs->db, qs, q, parent, i);
      parent = q;
    }
    MemoryBarrier();
    qs->blocks[b] = ql;
  }
  simpleMutexUnlock(&qs->lock);

  return &qs->blocks[b][depth];
}


static void
push_query(query *q)
{ simpleMutexLock(&q->stack->lock);
  q->stack->top++;
  simpleMutexUnlock(&q->stack->lock);
}


static void
pop_query(query *q)
{ simpleMutexLock(&q->stack->lock);
  q->stack->top--;
  simpleMutexUnlock(&q->stack->lock);
}



query *
open_query(rdf_db *db)
{ int tid = PL_thread_self();
  thread_info *ti = rdf_thread_info(db, tid);
  query *q = alloc_query(&ti->queries);
  gen_t g;

  q->type = Q_NORMAL;
  if ( (g=ti->queries.rd_gen) == GEN_UNDEF )
  { q->rd_gen = q->wr_gen = db->queries.generation;
  } else
  { q->rd_gen = g;
    q->wr_gen = ti->queries.wr_gen;
  }

  push_query(q);

  return q;
}


void
close_query(query *q)
{ wait_list *wl;

  if ( (wl=q->waiters) )
  { wait_list *n;

    q->waiters = NULL;
    for(; wl; wl=n)
    { n = wl->next;
      wakeup(wl->waiter);
      rdf_free(q->db, wl, sizeof(*wl));
    }
  }

  pop_query(q);
}


		 /*******************************
		 *	     ADMIN		*
		 *******************************/

void
init_query_admin(rdf_db *db)
{ query_admin *qa = &db->queries;

  memset(qa, 0, sizeof(*qa));
  qa->generation = 0;
  simpleMutexInit(&qa->query.lock);
  simpleMutexInit(&qa->write.lock);
}


		 /*******************************
		 *     TRIPLE MANIPULATION	*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
We have three basic triple manipulations:

  - Add triples
  - Delete triples
  - Copy triples to a new indexing schema (due to cloud split/merge)

add_triples() adds an array of  triples   to  the database, stepping the
database generation by 1. Calls to add triples must be synchronized with
other addition calls, but not  with   read  nor  delete operations. This
synchronization is needed because without we   cannot set the generation
for new queries to a proper value.

Hmmm. Really long writes will block  short   ones.  Can we do something?
Introduce a priority, possibly  automatically   on  #triples, less means
higher.

  - If blocked, set a `wants-priority-write'
  - Super checks this and
    1. Numbers already numbered triples into the future
    2. Release lock
    3. Re-acquire lock
    4. If all have been added, find the proper generation,
       renumber all triples and step the generation.

Or, hand them to a separate thread?

  + Only this thread manages the chains: no need for locking.
  - If a thread adds triples, it has to wait.  This means two-way
    communication.  --> probably too high overhead.

TBD: Generation management is different if we are in a transaction.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

int
add_triples(query *q, triple **triples, size_t count)
{ rdf_db *db = q->db;
  gen_t gen;
  triple **ep = triples+count;
  triple **tp;

  simpleMutexLock(&db->queries.write.lock);
  gen = db->queries.generation + 1;
  for(tp=triples; tp < ep; tp++)
  { (*tp)->lifespan.born = gen;
    (*tp)->lifespan.died = GEN_MAX;
    link_triple(db, *tp);
  }
  db->queries.generation = gen;
  simpleMutexUnlock(&db->queries.write.lock);
					/* TBD: broadcast */

  return TRUE;
}


int
del_triples(query *q, triple **triples, size_t count)
{ rdf_db *db = q->db;
  gen_t gen;
  triple **ep = triples+count;
  triple **tp;

  simpleMutexLock(&db->queries.write.lock);
  gen = db->queries.generation + 1;
  for(tp=triples; tp < ep; tp++)
  { (*tp)->lifespan.died = gen;
  }
  db->queries.generation = gen;
  simpleMutexUnlock(&db->queries.write.lock);
					/* TBD: broadcast */

  return TRUE;
}


/* copy_triples() copies triples whose indexing is out-of-date to the
   new index.  This must happen after an rdfs:subPropertyOf addition
   has joined two non-empty predicate clouds.
*/

int
copy_triples(query *q, predicate **p, size_t count)
{ return TRUE;
}
