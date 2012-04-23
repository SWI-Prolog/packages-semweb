/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2011 VU University Amsterdam

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
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/

#include "rdf_db.h"
#include "alloc.h"
#define GC_THREADS
#define DYNAMIC_MARKS
#include <gc/gc.h>
#include <gc/gc_mark.h>

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Triples are removed in two steps:

  - rdf_gc() removes triples died before the oldest active generation
    from the hash-chains. They are there left to be reclaimed by
    Boehm-GC.
  - Boehm-GC won't collect the triple as long as there are pointers
    (e.g., from Prolog choice-points or threads scanning the clause
    list) pointing to the triple.

Unfortunately, Boehm-GC is a  _conservative_   garbage  collector.  This
means that it may  incorrectly  (due  to   bits  being  recognised  as a
pointer) not release a triple. This is fine as long as it only affects a
small minority of the triples, but   if multiple consequtive triples are
removed, these form a chain of  garbage.   If  some  triple in the chain
cannot be removed,  the  remainder  of   the  chain  cannot  be removed.
Experiments show this works out poorly for the triple-store.

We fix this using a private mark procedure. This procedure is called for
triples *after* step (1) above. It first scans for the first life triple
(or NULL) and then replaces all next  pointers in the garbage chain with
a direct pointer to the life cell (or NULL). Note that these markers may
run concurrently, but in  a  stop-the-world   situation  this  is  not a
problem.

It is probably not even a problem   for the incremental GC scenario, but
this remains to be proven. As  long  as   we  use  Boehm-GC to deal with
deleted data, this is unlikely to become an issue.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void **triple_free_list;
static unsigned triple_kind;

triple *
alloc_triple(void)
{ triple *t;

  t = GC_generic_malloc(sizeof(*t), triple_kind);
  if ( t )
    GC_set_flags(t, GC_FLAG_UNCOLLECTABLE);

  return t;
}


void
unalloc_triple(triple *t, int linger)
{ if ( t )
  { assert(t->atoms_locked == FALSE);

    if ( linger )
    { t->lingering = TRUE;
      GC_clear_flags(t, GC_FLAG_UNCOLLECTABLE);
    } else
      GC_FREE(t);
  }
}


static void
mark_triple_next(triple *t, int icol)
{ triple *p, *e, *n;

  for(e=t; e && !e->lingering; e=e->tp.next[icol])
    ;
  for(p=t; p && !p->lingering; p=n)
  { n = p->tp.next[icol];
    p->tp.next[icol] = e;
  }
}


static struct GC_ms_entry *
mark_triple(GC_word *addr,
	    struct GC_ms_entry *mark_stack_ptr,
	    struct GC_ms_entry *mark_stack_limit,
	    GC_word env)
{ triple *t = (triple*)addr;
  int i;

  assert(t->lingering);

  for(i=0; i<INDEX_TABLES; i++)
    mark_triple_next(t, i);

  mark_stack_ptr = GC_MARK_AND_PUSH(t->predicate.r,
				    mark_stack_ptr, mark_stack_limit,
				    (void**)&t);
  if ( t->object_is_literal )
    mark_stack_ptr = GC_MARK_AND_PUSH(t->object.literal,
				      mark_stack_ptr, mark_stack_limit,
				      (void**)&t);

  return mark_stack_ptr;
}


int
init_alloc(void)
{ int proc;

  triple_free_list = GC_new_free_list();
  proc = GC_new_proc(mark_triple);
  triple_kind = GC_new_kind(triple_free_list, GC_MAKE_PROC(proc, 0), 0, 1);

  return TRUE;
}
