/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2012-2016, VU University Amsterdam
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

#include <config.h>
#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include "skiplist.h"

static int debuglevel;

#define DEBUG(n, g) do { if ( debuglevel >= n ) { g; } } while(0)

int
skiplist_debug(int new)
{ int old = debuglevel;
  debuglevel = new;
  return old;
}


#ifndef offsetof
#define offsetof(structure, field) ((size_t) &(((structure *)NULL)->field))
#endif

#define subPointer(p,n) (void*)((char*)(p)-(n))
#define addPointer(p,n) (void*)((char*)(p)+(n))

#define SIZEOF_SKIP_CELL_NOPLAYLOAD(n) \
	(offsetof(skipcell, next[n]))
#define SIZEOF_SKIP_CELL(sl, n) \
	((sl)->payload_size + SIZEOF_SKIP_CELL_NOPLAYLOAD(n))

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
We need a logarithmic distribution  the   for  the skiplist cell height.
Originally, we used random(), but it  appears   that  this is not thread
safe on MacOS 10.9 (Maverics), leading to   a very poor distribution and
100 times slower multi-threaded loading of RDF :-(

I considered using mrand48(), but  the  Linux   man  pages  say  this is
deprecated and new code should use rand().  The manual page also gives a
reference implementation. We used this and turned   it  into a lock free
and thread-safe version. This avoids  surprises over different platforms
and ensures this remains portable.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static unsigned int next = 1536581061;

static unsigned int
my_rand(void)
{ unsigned int n, n0;

  do
  { n0 = next;

    n = n0 * 1103515245 + 12345;
  } while(n != n0 && !__sync_bool_compare_and_swap(&next, n0, n));

  return((unsigned int)(n/65536) % 32768);
}


static int
cell_height(void)
{ long r;
  int h = 1;

  r  = my_rand();
  if ( r == 32767 )			/* all 1-s, create more bits */
    r = my_rand()<<15;

  while(r&0x1)
  { h++;
    r >>= 1;
  }

  return h;
}


static void *
sl_malloc(size_t bytes, void *client_data)
{ return malloc(bytes);
}


void
skiplist_init(skiplist *sl, size_t payload_size,
	      void *client_data,
	      int  (*compare)(void *p1, void *p2, void *cd),
	      void*(*alloc)(size_t bytes, void *cd),
	      void (*destroy)(void *p, void *cd))
{ memset(sl, 0, sizeof(*sl));

  if ( !alloc )
    alloc = sl_malloc;

  sl->client_data  = client_data;
  sl->payload_size = payload_size;
  sl->compare      = compare;
  sl->alloc        = alloc;
  sl->destroy      = destroy;
  sl->height       = 1;
  sl->count        = 0;
}


void
skiplist_destroy(skiplist *sl)
{ int h=0;
  void **scp;

  scp = (void**)sl->next[h];
  while(scp)
  { void **next = (void**)*scp;
    skipcell *sc = subPointer(scp, SIZEOF_SKIP_CELL_NOPLAYLOAD(h));
    void *cell_payload = subPointer(sc, sl->payload_size);

    if ( sl->destroy )
      (*sl->destroy)(cell_payload, sl->client_data);

    scp = next;
  }
}


skipcell *
new_skipcell(skiplist *sl, void *payload)
{ int h = cell_height();
  char *p = (*sl->alloc)(SIZEOF_SKIP_CELL(sl, h), sl->client_data);

  if ( p )
  { skipcell *sc = (skipcell*)(p+sl->payload_size);

    DEBUG(1, Sdprintf("Allocated payload at %p; cell at %p\n", p, sc));

    memcpy(p, payload, sl->payload_size);
    sc->height = h;
    sc->erased = FALSE;
    sc->magic = SKIPCELL_MAGIC;
    memset(sc->next, 0, sizeof(*sc->next)*h);

    return sc;
  }

  return NULL;
}


void *
skiplist_find(skiplist *sl, void *payload)
{ int h = sl->height-1;			/* h>=1 */
  void **scp, **scpp;

  scp = &sl->next[h];
  scpp = NULL;

  while(h>=0)
  { void *nscp;

    if ( scpp )
    { skipcell *sc = subPointer(scp, SIZEOF_SKIP_CELL_NOPLAYLOAD(h));
      void *cell_payload = subPointer(sc, sl->payload_size);
      int diff = (*sl->compare)(payload, cell_payload, sl->client_data);

      assert(sc->magic == SKIPCELL_MAGIC);

      if ( diff == 0 )
      { if ( !sc->erased )
	  return cell_payload;
	else
	  return NULL;
      } else if ( diff < 0 )		/* cell payload > target */
      { do
	{ scpp--;
	  scp = (void**)*scpp;
	  h--;
	} while(h>=0 && scp == NULL);

	continue;
      }
    }

    if ( (nscp = *scp) )
    { scpp = scp;
      scp  = (void**)nscp;
    } else
    { if ( scpp )
	scpp--;
      scp--;
      h--;
    }
  }

  return NULL;
}

/* Find first cell with key >= payload.  Returns first cell if payload == NULL
*/

void *
skiplist_find_first(skiplist *sl, void *payload, skiplist_enum *en)
{ void **scp;
  int h;

  en->list = sl;

  if ( payload )
  { void **scpp, *nscp;

    h    = sl->height-1;			/* h>=1 */
    scp  = &sl->next[h];
    scpp = NULL;

    while(h>=0)
    { if ( scpp )
      { skipcell *sc = subPointer(scp, SIZEOF_SKIP_CELL_NOPLAYLOAD(h));
	void *cell_payload = subPointer(sc, sl->payload_size);
	int diff = (*sl->compare)(payload, cell_payload, sl->client_data);

	assert(sc->magic == SKIPCELL_MAGIC);

	if ( diff == 0 )
	{ goto found;
	} else if ( diff < 0 )		/* cell payload > target */
	{ if ( h == 0 )
	    goto found;

	  do
	  { scpp--;
	    scp = (void**)*scpp;
	    h--;
	  } while(scp == NULL && h>=0 );

	  continue;
	}
      }

      if ( (nscp = *scp) )
      { scpp = scp;
	scp  = (void**)nscp;
      } else
      { if ( scpp )
	  scpp--;
	scp--;
	h--;
      }
    }

    return NULL;
  } else
  { scp = (void**)sl->next[0];
    h = 0;

    if ( scp )
    { skipcell *sc;

    found:
      sc = subPointer(scp, SIZEOF_SKIP_CELL_NOPLAYLOAD(h));
      assert(sc->magic == SKIPCELL_MAGIC);

      if ( (scp = sc->next[0]) )
      { en->current = subPointer(scp, SIZEOF_SKIP_CELL_NOPLAYLOAD(0));
      } else
      { en->current = NULL;
      }

      if ( !sc->erased )
      { void *cell_payload = subPointer(sc, sl->payload_size);
	return cell_payload;
      } else
      { return skiplist_find_next(en);
      }
    }

    return NULL;
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
skiplist_find_next() returns the payload of the  next cell. Note that if
cells are deleted, they may not  be discarded. I.e., this implementation
depends on Boehm-GC to avoid this.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

void *
skiplist_find_next(skiplist_enum *en)
{ skipcell *sc;
  void **scp;
  void *cell_payload;

  do
  { if ( !(sc = en->current) )
      return NULL;

    if ( (scp = sc->next[0]) )
    { en->current = subPointer(scp, SIZEOF_SKIP_CELL_NOPLAYLOAD(0));
    } else
    { en->current = NULL;
    }
  } while( sc->erased );

  cell_payload = subPointer(sc, en->list->payload_size);

  return cell_payload;
}


void
skiplist_find_destroy(skiplist_enum *en)
{
}


int
skiplist_check(skiplist *sl, int print)
{ int h;

  for(h = SKIPCELL_MAX_HEIGHT-1; h>=0; h--)
  { void **scp  = (void*)sl->next[h];
    void **scpp = NULL;
    int count = 0;

    while(scp)
    { skipcell *sc = subPointer(scp, SIZEOF_SKIP_CELL_NOPLAYLOAD(h));
      assert(sc->magic == SKIPCELL_MAGIC);

      count++;

      if ( h == 0 )
      { int i;

	for(i=1; i<sc->height; i++)
	{ if ( sc->next[i] )
	  { skipcell *next0 = subPointer(sc->next[i-1],
					 SIZEOF_SKIP_CELL_NOPLAYLOAD(i-1));
	    skipcell *next1 = subPointer(sc->next[i],
					 SIZEOF_SKIP_CELL_NOPLAYLOAD(i));
	    void *p0, *p1;
	    assert(next0->magic == SKIPCELL_MAGIC);
	    assert(next1->magic == SKIPCELL_MAGIC);
	    p0 = subPointer(next0, sl->payload_size);
	    p1 = subPointer(next1, sl->payload_size);

	    assert((*sl->compare)(p0, p1, sl->client_data) <= 0);
	  }
	}
      }

      if ( scpp )
      { void *pl1, *pl2;

	skipcell *prev = subPointer(scpp, SIZEOF_SKIP_CELL_NOPLAYLOAD(h));
	assert(prev->magic == SKIPCELL_MAGIC);
	pl1 = subPointer(prev, sl->payload_size);
	pl2 = subPointer(sc,   sl->payload_size);
	assert((*sl->compare)(pl1, pl2, sl->client_data) < 0);
      }

      scpp = scp;
      scp = (void**)*scp;
    }

    if ( print )
      Sdprintf("%-4d: %-4d\n", h, count);
  }

  return TRUE;
}


void *
skiplist_insert(skiplist *sl, void *payload, int *is_new)
{ void *rc;

  if ( !(rc=skiplist_find(sl, payload)) )
  { skipcell *new = new_skipcell(sl, payload);
    int h;
    void **scp, **scpp;

    if ( new->height > sl->height )
      sl->height = new->height;

    h    = sl->height-1;
    scp  = &sl->next[h];
    scpp = NULL;

    DEBUG(2, Sdprintf("Inserting new cell %p of height %d\n", new, new->height));

    while(h>=0)
    { if ( scpp )
      { skipcell *sc = subPointer(scp, SIZEOF_SKIP_CELL_NOPLAYLOAD(h));
	void *cell_payload = subPointer(sc, sl->payload_size);
	int diff = (*sl->compare)(payload, cell_payload, sl->client_data);

	assert(sc->magic == SKIPCELL_MAGIC);
	DEBUG(2, Sdprintf("Cell payload at %p\n", cell_payload));

	assert(diff != 0);

	if ( diff < 0 )			/* cell payload > target */
	{ if ( h < new->height )
	  { DEBUG(3, Sdprintf("Between %p and %p at height = %d\n",
			      scpp, scp, h));
	    new->next[h] = scp;
	    *scpp = &new->next[h];
	    scpp--;
	    scp = (void**)*scpp;
	    h--;
	  } else			/* new one is too low */
	  { scpp--;
	    scp = (void**)*scpp;
	    h--;
	  }
	  continue;
	}
      }

      if ( *scp == NULL )
      { if ( new->height > h )
	  *scp = &new->next[h];
	if ( scpp )
	  scpp--;
	scp--;
	h--;
	continue;
      }

      scpp = scp;
      scp  = (void**)*scp;
    }
    sl->count++;

    DEBUG(1, skiplist_check(sl, FALSE));
    if ( is_new )
      *is_new = TRUE;

    return subPointer(new, sl->payload_size);
  }

  if ( is_new )
    *is_new = FALSE;

  return rc;
}


void *
skiplist_delete(skiplist *sl, void *payload)
{ int h = sl->height-1;
  void **scp, **scpp;

  if ( h < 0 )
    return NULL;			/* empty */

  scp = &sl->next[h];
  scpp = NULL;

  while(h>=0)
  { if ( scpp )
    { skipcell *sc = subPointer(scp, SIZEOF_SKIP_CELL_NOPLAYLOAD(h));
      void *cell_payload = subPointer(sc, sl->payload_size);
      int diff = (*sl->compare)(payload, cell_payload, sl->client_data);

      assert(sc->magic == SKIPCELL_MAGIC);

      if ( diff == 0 )
      { sc->erased = TRUE;

	*scpp = (void**)*scp;
	if ( h > 0 )
	{ scpp--;
	  scp = (void**)*scpp;
	  h--;
	  continue;
	} else
	{ sl->count--;

	  return cell_payload;
	}
      } else if ( diff < 0 )		/* cell payload > target */
      { scpp--;
	scp = (void**)*scpp;
	h--;
	continue;
      }
    }

    if ( *scp == NULL )
    { if ( scpp )
	scpp--;
      scp--;
      h--;
      continue;
    }

    scpp = scp;
    scp  = (void**)*scp;
  }

  return NULL;
}


/* skiplist_erased_payload() returns whether or not the cell associated
   with payload is erased or not. This may only be used on payloads
   returned from one of the skiplist calls.
*/

int
skiplist_erased_payload(skiplist *sl, void *payload)
{ skipcell *sc = addPointer(payload, sl->payload_size);

  return sc->erased;
}
