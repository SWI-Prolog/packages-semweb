/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2012 VU University Amsterdam

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

#ifndef PL_DEFER_FREE_H_INCLUDED
#define PL_DEFER_FREE_H_INCLUDED

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
This header supports freeing data in   datastructures  that are designed
such that they can  be  read   without  locking  concurrently with write
operations. That is, typically write operations  use mutex based locking
to avoid conflicts, but the write   operations  to the datastructure are
carefully ordered to allow readers to work concurrently.

For example, an element can be  removed   safely  from  a linked list by
making the previous cell point  to  the   next.  The  problem is that we
cannot free the list cell because some thread may be traversing it. This
thread will now follow an  invalid  next   pointer.  That  is where this
library comes in. It demands readers to wrap their dangerous work in the
sequence below instead of acquiring a lock:

    enter_scan(handle)
    ...
    exit_scan(handle)

And, it demands writers to call the following rather than PL_free():

    deferred_free(handle, ptr)
    deferred_finalize(handle, ptr,
		      (*finalizer)(void*mem, void*client_data),
		      client_data)

Actual freeing the  objects  is  deferred   until  there  are  no readers
scanning the object.  Note  that  this   is  too  strong  a requirement.
Ideally, we'd pick the free list  and   wait  until all threads have had
some point where they finished all their scanning activities. I.e., this
schema leads to long defers actually freeing  memory if there are almost
continuously threads that batter a datastructure.

TODO:

  - The current datastructure keeps a linked list of free defer-cells.
  We should somehow clear up this list if it gets too big. This should
  be doable by atomically removing it from the free structure and
  deleting it.

  - If we discover that we do not get into an inactive state for a long
  time, we should somehow switch to a different technique. The different
  technique may imply that we pick the free list and wait until all
  threads have finished their scan.  Not sure there is a fairly elegant
  way to switch between the two techniques.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

/* TODO: Use tagged pointers to have both finalized destruction and
   plain simple destruction?
*/

typedef struct defer_cell
{ struct defer_cell *next;
  void		    *mem;			/* guarded memory */
  void		   (*finalizer)(void*mem, void*client_data);
  void		    *client_data;
} defer_cell;

typedef struct defer_free
{ unsigned int	active;				/* Active users */
  defer_cell   *free_cells;			/* List if free cells */
  defer_cell   *freed;				/* Freed objects */
  size_t	allocated;			/* Allocated free cells */
} defer_free;


#define FREE_CHUNK_SIZE 256

static defer_cell *
new_cells(defer_free *df, defer_cell **lastp)
{ defer_cell *c = malloc(sizeof(*c)*FREE_CHUNK_SIZE);

  if ( c )
  { defer_cell *n, *last = &c[FREE_CHUNK_SIZE-1];

    for(n=c; n != last; n++)
      n->next = n+1;
    last->next = NULL;
    *lastp = last;

    df->allocated += FREE_CHUNK_SIZE;		/* not locked; but just stats */
  }

  return c;
}


static void
free_defer_list(defer_free *df, defer_cell *list, defer_cell *last)
{ defer_cell *o;

  do
  { o = df->free_cells;
    last->next = o;
  } while ( !__sync_bool_compare_and_swap(&df->free_cells, o, list) );
}


static inline defer_cell *
alloc_defer_cell(defer_free *df)
{ defer_cell *c;

  do
  { c = df->free_cells;
    if ( !c )
    { defer_cell *last;
      defer_cell *fl = new_cells(df, &last);

      if ( fl )
      { free_defer_list(df, fl, last);
	c = df->free_cells;
      } else
	return NULL;
    }
  } while ( !__sync_bool_compare_and_swap(&df->free_cells, c, c->next) );

  return c;
}


/* TBD: what to do of alloc_defer_cell() return NULL?
*/

static inline void
deferred_free(defer_free *df, void *data)
{ defer_cell *c = alloc_defer_cell(df);
  defer_cell *o;

  c->mem       = data;
  c->finalizer = NULL;

  do
  { o = df->freed;
    c->next = o;
  } while ( !__sync_bool_compare_and_swap(&df->freed, o, c) );
}


static inline void
deferred_finalize(defer_free *df, void *data,
		  void (*finalizer)(void *data, void *client_data),
		  void *client_data)
{ defer_cell *c = alloc_defer_cell(df);
  defer_cell *o;

  c->mem	 = data;
  c->finalizer	 = finalizer;
  c->client_data = client_data;

  do
  { o = df->freed;
    c->next = o;
  } while ( !__sync_bool_compare_and_swap(&df->freed, o, c) );
}



static inline void
enter_scan(defer_free *df)
{ __sync_add_and_fetch(&df->active, 1);
}


static inline void
exit_scan(defer_free *df)
{ defer_cell *o = df->freed;

  if ( __sync_sub_and_fetch(&df->active, 1) == 0 )
  { if ( o && __sync_bool_compare_and_swap(&df->freed, o, NULL) )
    { defer_cell *fl = o;

      for(;;)
      { if ( o->finalizer )
	  (*o->finalizer)(o->mem, o->client_data);
	free(o->mem);

	if ( o->next )
	{ o = o->next;
	} else
	{ free_defer_list(df, fl, o);
	  break;
	}
      }
    }
  }
}

#endif /*PL_DEFER_FREE_H_INCLUDED*/
