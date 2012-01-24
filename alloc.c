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
  { if ( linger )
      GC_clear_flags(t, GC_FLAG_UNCOLLECTABLE);
    else
      GC_FREE(t);
  }
}


static void
mark_triple_next(triple *t, int icol)
{ triple *p, *e, *n;

  for(e=t; e && !e->atoms_locked; e=e->tp.next[icol])
    ;
  for(p=t; p && !p->atoms_locked; p=n)
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

  assert(t->atoms_locked == FALSE);		/* TBD: Unclear name for erased */

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
