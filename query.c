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
#include "query.h"
#include "memory.h"

query *
open_query(rdf_db *db)
{ query *q = rdf_malloc(db, sizeof(*q));

  memset(q, 0, sizeof(*q));

  q->db          = db;
  q->thread      = PL_thread_self();
  q->thread_info = rdf_thread_info(db, q->thread);
  if ( q->thread_info->transaction )
  { q->generation = q->thread_info->transaction->generation;
  } else
  { q->generation = db->queries.generation;
  }

  return q;
}


gen_t
query_write_generation(query *q)
{ if ( q->thread_info->transaction )
    return q->thread_info->transaction.trans.generation;
  else
    return q->db->queries.generation;
}




/* Return the per-thread data for the given DB.  This uses a lock-free
   algorithm that is also used by SWI-Prolog thread-local predicates.
*/

thread_info *
rdf_thread_info(rdf_db *db, int tid)
{ per_thread *td = &db->queries.per_thread;
  size_t idx = MSB(tid);

  if ( !td->blocks[idx] )
  { LOCK_DB(db);
    if ( !td->blocks[idx] )
    { size_t bs = (size_t)1<<idx;
      thread_info *newblock = allocHeap(bs*sizeof(thread_info));

      memset(newblock, 0, bs*sizeof(thread_info));

      td->blocks[idx] = newblock-bs;
    }
    UNLOCK_DB(db);
  }

  return &td->blocks[idx][tid];
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
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

int
add_triples(rdf_db *db, triple **triples, size_t count)
{ gen_t gen = db->queries.generation + 1;
  triple **ep = triples+count;
  triple **tp;

  LOCK(db->queries.write.lock);
  for(tp=triples; tp < ep; tp++)
  { (*tp)->born = gen;
    (*tp)->died = GEN_MAX;
    link_triple(db, *tp);
  }
  db->queries.generation++;
  UNLOCK(db->queries.write.lock);

  return TRUE;
}


int
del_triples(rdf_db *db, triple **triples, size_t count)
{ gen_t gen = db->queries.generation + 1;
  triple **ep = triples+count;
  triple **tp;

  LOCK(db->queries.write.lock);
  for(tp=triples; tp < ep; tp++)
  { (*tp)->died = gen;
    link_triple(db, *tp);
  }
  db->queries.generation++;
  UNLOCK(db->queries.write.lock);

  return TRUE;
}


/* copy_triples() copies triples whose indexing is out-of-date to the
   new index.  This must happen after an rdfs:subPropertyOf addition
   has joined two non-empty predicate clouds.
*/

int
copy_triples(rdf_db *db, predicate **p, size_t count)
{
}
