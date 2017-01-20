/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2006-2015, University of Amsterdam
                              VU University Amsterdam
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

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

/*#define O_SECURE 1*/
#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include "deferfree.h"
#include "skiplist.h"
#include "mutex.h"
#include "memory.h"
#include "atom.h"
#include "murmur.h"
#include "debug.h"
#include <string.h>
#include <assert.h>
#ifdef __WINDOWS__
#define inline __inline
#endif

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
This file realises the low-level support   for  indexing literals in the
semantic web library. The idea is to   make a map from abstracted tokens
from each literal to  the  exact   literals.  Abstraction  introduces  a
certain amount of ambiguity that  makes   fuzzy  matching possible. Good
abstraction candidates are the Porter Stem or Snowball algorithm and the
Double Metaphone algorithm. Both  are  provide   by  the  SWI-Prolog NLP
package.

Basic query provides a  set  of   abstracted  terms  and  requests those
literals containing all of them. We   maintain  ordered sets of literals
and do set-intersection on them to achieve good linear performance.

Some current E-culture project statistics (porter stem)

	  # stems: 0.4 million
	  # literals: 0.9 million
	  # stem->literal relations: 3.1 million

Av. literals/stem: about 8.

Searching is done using

	rdf_find_literal_map(Map, SetOfAbstract, -Literals)
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			      TODO

    - The set must be handled by a Prolog symbol to ensure clean
      destruction.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */


		 /*******************************
		 *	    PRIVATE DATA	*
		 *******************************/

#define AM_MAGIC	0x6ab19e8e

typedef struct atom_map
{ int		magic;			/* AM_MAGIC */
  size_t	value_count;		/* total # values in all keys */
  simpleMutex	lock;			/* Multi-threaded access */
  skiplist	list;			/* Skip list */
  defer_free	defer;			/* Concurrent free handling */
  float		existing;		/* Locking policy */
  float		new;			/* Locking policy */
} atom_map;

typedef void *datum;

#define S_MAGIC 0x8734abcd

typedef struct atom_chash
{ size_t allocated;
  datum  atoms[1];
} atom_chash;

#define SIZEOF_ATOM_HASH(n)	offsetof(atom_chash, atoms[n])

typedef struct atom_set
{ size_t	size;			/* # cells in use */
  atom_chash    *entries;		/* Close hash table */
#ifdef O_SECURE
  int	  magic;
#endif
} atom_set;


#define ND_MAGIC    0x67b49a20
#define ND_MAGIC_EX 0x67b49a21

typedef struct node_data
{ datum		key;
  atom_set      values;
#ifdef O_SECURE
  int		magic;
#endif
} node_data;

typedef struct node_data_ex
{ node_data	data;
  atom_info	atom;
#ifdef O_SECURE
  int		magic;
#endif
} node_data_ex;


#define LOCK(map)			simpleMutexLock(&map->lock)
#define UNLOCK(map)			simpleMutexUnlock(&map->lock)


		 /*******************************
		 *	     BASIC STUFF	*
		 *******************************/

static functor_t FUNCTOR_atom_map1;
static functor_t FUNCTOR_size2;
static functor_t FUNCTOR_not1;
static atom_t	 ATOM_all;
static atom_t	 ATOM_case;
static atom_t	 ATOM_prefix;
static atom_t	 ATOM_le;
static atom_t	 ATOM_ge;
static atom_t	 ATOM_between;
static atom_t	 ATOM_key;

#define MKFUNCTOR(n,a) \
	FUNCTOR_ ## n ## a = PL_new_functor(PL_new_atom(#n), a)
#define MKATOM(n) \
	ATOM_ ## n = PL_new_atom(#n)

static void
init_functors()
{ FUNCTOR_atom_map1 = PL_new_functor(PL_new_atom("$literal_map"), 1);

  MKFUNCTOR(size, 2);
  MKFUNCTOR(not, 1);

  MKATOM(all);
  MKATOM(case);
  MKATOM(prefix);
  MKATOM(le);
  MKATOM(ge);
  MKATOM(between);
  MKATOM(key);
}


static int
get_atom_map(term_t t, atom_map **map)
{ if ( PL_is_functor(t, FUNCTOR_atom_map1) )
  { term_t a = PL_new_term_ref();
    void *ptr;

    _PL_get_arg(1, t, a);
    if ( PL_get_pointer(a, &ptr) )
    { atom_map *am = ptr;

      if ( am->magic == AM_MAGIC )
      { *map = am;
        return TRUE;
      }
    }
  }

  return PL_type_error("atom_map", t);
}


static int
unify_atom_map(term_t t, atom_map *map)
{ return PL_unify_term(t, PL_FUNCTOR, FUNCTOR_atom_map1,
		            PL_POINTER, map);
}


		 /*******************************
		 *	       DATUM		*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Datum is either an atom or a 31-bit  signed integer. Atoms are shifted 7
bits
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#define ATOM_TAG_BITS 7
#define ATOM_TAG 0x1
#define EMPTY ((datum)ATOM_TAG)

#define tag(d)		((intptr_t)(d)&0x1)
#define isAtomDatum(d)  ((intptr_t)(d)&ATOM_TAG)
#define isIntDatum(d)	!isAtomDatum(d)

#define MAP_MIN_INT	(-(intptr_t)((intptr_t)1<<(sizeof(intptr_t)*8 - 1 - 1)))
#define MAP_MAX_INT	(-MAP_MIN_INT - 1)

static intptr_t atom_mask;

static void
init_datum_store(void)
{ atom_t a = PL_new_atom("[]");

  atom_mask = a & ((1<<(ATOM_TAG_BITS-1))-1);
}


static inline atom_t
atom_from_datum(datum d)
{ uintptr_t v = (uintptr_t)d;
  atom_t a;

  a  = ((v&~0x1)<<(ATOM_TAG_BITS-1))|atom_mask;
  DEBUG(9, Sdprintf("0x%lx --> %s\n", v, PL_atom_chars(a)));
  return a;
}


static inline intptr_t
integer_from_datum(datum d)
{ intptr_t v = (intptr_t)d;

  return (v>>1);
}


static inline datum
atom_to_datum(atom_t a)
{ uintptr_t v = (a>>(ATOM_TAG_BITS-1))|ATOM_TAG;

  SECURE(assert(atom_from_datum((datum)v) == a));
  DEBUG(9, Sdprintf("Atom %s --> 0x%lx\n", PL_atom_chars(a), v));

  return (datum)v;
}


static inline datum
integer_to_datum(intptr_t v)
{ return (datum)(v<<1);
}


static int
get_datum(term_t t, datum* d)
{ atom_t a;
  intptr_t l;

  if ( PL_get_atom(t, &a) )
  { *d = atom_to_datum(a);
    return TRUE;
  } else if ( PL_get_intptr(t, &l) )
  { if ( l < MAP_MIN_INT || l > MAP_MAX_INT )
      return PL_representation_error("integer_range");

    *d = integer_to_datum(l);
    return TRUE;
  }

  return PL_type_error("atom or integer", t);
}


static int
get_search_datum(term_t t, node_data_ex *search)
{ atom_t a;
  intptr_t l;

  SECURE(search->magic = ND_MAGIC_EX);

  if ( PL_get_atom(t, &a) )
  { search->data.key = atom_to_datum(a);
    search->atom.handle   = a;
    search->atom.resolved = FALSE;
    return TRUE;
  } else if ( PL_get_intptr(t, &l) )
  { if ( l < MAP_MIN_INT || l > MAP_MAX_INT )
      return PL_representation_error("integer_range");

    search->data.key = integer_to_datum(l);
    return TRUE;
  }

  return PL_type_error("atom or integer", t);
}


static int
unify_datum(term_t t, datum d)
{ uintptr_t v = (uintptr_t)d;

  if ( isAtomDatum(v) )
    return PL_unify_atom(t, atom_from_datum(d));
  else
    return PL_unify_integer(t, integer_from_datum(d));
}


static void
lock_datum(datum d)
{ uintptr_t v = (uintptr_t)d;

  if ( isAtomDatum(v) )
    PL_register_atom(atom_from_datum(d));
}


static void
unlock_datum(datum d)
{ if ( d != EMPTY )
  { uintptr_t v = (uintptr_t)d;

    if ( isAtomDatum(v) )
      PL_unregister_atom(atom_from_datum(d));
  }
}


static const char *
format_datum(datum d, char *buf)
{ static char tmp[20];

  if ( isAtomDatum(d) )
    return PL_atom_chars(atom_from_datum(d));

  if ( !buf )
    buf = tmp;
  Ssprintf(buf, "%lld", (int64_t)integer_from_datum(d));

  return buf;
}



		 /*******************************
		 *	     ATOM SETS		*
		 *******************************/

static int insert_atom_set(atom_map *map, atom_set *as, datum a);
static int insert_atom_hash(atom_chash *hash, datum add);

#define AS_INITIAL_SIZE 4

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
A set of datums  (atoms  or  integers)   is  a  closed  hash-table.  The
implementation is an adapted copy from   XPCE's  class hash_table, using
closed hash-tables.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
init_atom_set(atom_map *map, atom_set *as, datum a0)
{ if ( (as->entries = malloc(SIZEOF_ATOM_HASH(AS_INITIAL_SIZE))) )
  { size_t i;

    as->size = 0;
    as->entries->allocated = AS_INITIAL_SIZE;
    for(i=0; i<AS_INITIAL_SIZE; i++)
      as->entries->atoms[i] = EMPTY;

    insert_atom_set(map, as, a0);
    lock_datum(a0);

    return TRUE;
  }

  return FALSE;
}


static unsigned int
hash_datum(datum d)
{ return rdf_murmer_hash(&d, sizeof(d), MURMUR_SEED);
}


#ifdef O_SECURE
static int
at_least_one_empty(atom_chash *ah)
{ datum *p = ah->atoms;
  datum *e = &ah->atoms[ah->allocated];

  for(; p<e; p++)
  { if ( *p == EMPTY )
      return TRUE;
  }

  return FALSE;
}
#endif


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
in_atom_set(atom_set *as, datum a)

returns TRUE if datum is in the  set. This function can run concurrently
with insert/delete.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
in_atom_set(atom_set *as, datum a)
{ atom_chash *snap = as->entries;
  unsigned int start = hash_datum(a) % snap->allocated;
  datum *d = &snap->atoms[start];
  datum *e = &snap->atoms[snap->allocated];

  for(;;)
  { if ( *d == a )
      return TRUE;
    if ( *d == EMPTY )
      return FALSE;
    if ( ++d == e )
      d = snap->atoms;
  }

  return FALSE;
}


static int
resize_atom_set(atom_map *map, atom_set *as, size_t size)
{ atom_chash *new = malloc(SIZEOF_ATOM_HASH(size));

  if ( new )
  { size_t i;
    datum *p = as->entries->atoms;
    datum *e = &p[as->entries->allocated];
    atom_chash *old;

    new->allocated = size;
    for(i=0; i<size; i++)
      new->atoms[i] = EMPTY;

    for(; p<e; p++)
    { if ( *p != EMPTY )
	insert_atom_hash(new, *p);
    }

    old = as->entries;
    as->entries = new;			/* must be synchronized */
    deferred_free(&map->defer, old);	/* leave to GC */

    return TRUE;
  }

  return FALSE;
}


static int
insert_atom_hash(atom_chash *hash, datum add)
{ datum *d = &hash->atoms[hash_datum(add) % hash->allocated];
  datum *e = &hash->atoms[hash->allocated];

  for(;;)
  { if ( *d == add )
      return 0;					/* nothing added */
    if ( *d == EMPTY )
    { *d = add;
      return 1;
    }
    if ( ++d == e )
      d = hash->atoms;
  }
}


/* Adds a datum to the set.  Returns 0 if nothing was added; 1 if the
   datum was added or -1 if we are out of memory.
*/

static int
insert_atom_set(atom_map *map, atom_set *as, datum a)
{ int rc;

  if ( 4*(as->size+1) > 3*as->entries->allocated )
  { if ( !resize_atom_set(map, as, 2*as->entries->allocated) )
      return -1;				/* no memory */
  }

  rc = insert_atom_hash(as->entries, a);
  as->size += rc;
  SECURE(assert(at_least_one_empty(as->entries)));

  return rc;
}


static int
delete_atom_set(atom_map *map, atom_set *as, datum a)
{ unsigned int i;
  int j, r;

  if ( as->size < as->entries->allocated/4 &&
       as->entries->allocated > AS_INITIAL_SIZE )
  { if ( !resize_atom_set(map, as, as->entries->allocated/2) )
      return -1;				/* no memory */
  }

  i = hash_datum(a) % as->entries->allocated;

  while(as->entries->atoms[i] != EMPTY && as->entries->atoms[i] != a)
  { if ( ++i == as->entries->allocated )
      i = 0;
  }
  if ( as->entries->atoms[i] == EMPTY )
    return FALSE;				/* not in table */

  as->size--;
  as->entries->atoms[i] = EMPTY;		/* R1 */
  j = i;

  for(;;)
  { if ( ++i == as->entries->allocated )
      i = 0;

    if ( as->entries->atoms[i] == EMPTY )
      break;

    r = hash_datum(as->entries->atoms[i]) % as->entries->allocated;
    if ( (i >= r && r > j) || (r > j && j > i) || (j > i && i >= r) )
      continue;

    as->entries->atoms[j] = as->entries->atoms[i];
    SECURE(assert(at_least_one_empty(as->entries)));
    as->entries->atoms[i] = EMPTY;
    j = i;
  }

  SECURE(assert(at_least_one_empty(as->entries)));
  return TRUE;
}


static void
destroy_atom_set(atom_set *as)
{ size_t i;

  for(i=0; i<as->entries->allocated; i++)
    unlock_datum(as->entries->atoms[i]);

  free(as->entries);
}


static void
finalize_atom_set(atom_map *am, atom_set *as)
{ size_t i;

  for(i=0; i<as->entries->allocated; i++)
    unlock_datum(as->entries->atoms[i]);

  deferred_free(&am->defer, as->entries);
}


static void
free_node_data(void *ptr, void *cd)
{ node_data *data = ptr;
  atom_map *am = cd;

  DEBUG(2,
	char b[20];
	Sdprintf("Destroying node with key = %s\n",
		 format_datum(data->key, b)));

  unlock_datum(data->key);
  finalize_atom_set(am, &data->values);
}


		 /*******************************
		 *	   TREE INTERFACE	*
		 *******************************/

static int
cmp_node_data(void *l, void *r, void *cd)
{ node_data_ex *e1 = l;
  node_data *n2 = r;
  datum *d1 = e1->data.key;
  datum *d2 = n2->key;
  int d;
  (void)cd;

  SECURE(assert(e1->magic == ND_MAGIC_EX));

  if ( (d=(tag(d1)-tag(d2))) == 0 )
  { if ( isAtomDatum(d1) )
    { return cmp_atom_info(&e1->atom, atom_from_datum(d2));
    } else
    { intptr_t l1 = integer_from_datum(d1);
      intptr_t l2 = integer_from_datum(d2);

      return l1 > l2 ? 1 : l1 < l2 ? -1 : 0;
    }
  }

  return d;
}


static void *
map_alloc(size_t size, void *cd)
{ (void)cd;

  return malloc(size);
}


static void
init_map(atom_map *m)
{ skiplist_init(&m->list,
		sizeof(node_data),	/* Payload size */
		m,			/* Client data */
		cmp_node_data,		/* Compare */
		map_alloc,		/* Allocate */
		free_node_data);	/* Destroy */
}


static foreign_t
new_atom_map(term_t handle)
{ atom_map *m;

  if ( !(m=malloc(sizeof(*m))) )
    return PL_resource_error("memory");

  memset(m, 0, sizeof(*m));
  simpleMutexInit(&m->lock);
  init_map(m);
  m->magic = AM_MAGIC;

  return unify_atom_map(handle, m);
}


static foreign_t
destroy_atom_map(term_t handle)
{ atom_map *m;

  if ( !get_atom_map(handle, &m) )
    return FALSE;

  LOCK(m);
  if ( m->defer.active )
  { UNLOCK(m);
    return PL_permission_error("destroy", "atom_map", handle);
  }
  m->magic = 0;
  skiplist_destroy(&m->list);
  UNLOCK(m);
  simpleMutexDelete(&m->lock);
  free(m);

  return TRUE;
}


		 /*******************************
		 *	       INSERT		*
		 *******************************/


static foreign_t
insert_atom_map4(term_t handle, term_t from, term_t to, term_t keys)
{ atom_map *map;
  datum a2;
  node_data_ex search;
  node_data *data;

  if ( !get_atom_map(handle, &map) ||
       !get_search_datum(from, &search) ||
       !get_datum(to, &a2) )
    return FALSE;

  enter_scan(&map->defer);
  if ( (data=skiplist_find(&map->list, &search)) )
  { int rc;

    SECURE(assert(data->magic == ND_MAGIC));

    LOCK(map);
    rc = ( !skiplist_erased_payload(&map->list, data) &&
	   insert_atom_set(map, &data->values, a2)
	 );
    if ( rc )
    { lock_datum(a2);			/* Must be locked because it may */
					/* otherwise be deleted concurrently */
      map->value_count++;
    }
    UNLOCK(map);

    if ( rc < 0 )
    { exit_scan(&map->defer);
      return PL_resource_error("memory");
    }
  } else
  { int is_new;

    if ( keys && !PL_unify_integer(keys, map->list.count+1) )
    { exit_scan(&map->defer);
      return FALSE;
    }
    if ( !init_atom_set(map, &search.data.values, a2) )
    { exit_scan(&map->defer);
      return PL_resource_error("memory");
    }

    if ( map->existing*2 > map->new &&
	 (data=skiplist_find(&map->list, &search)) )
    { LOCK(map);
      if ( !skiplist_erased_payload(&map->list, data) )
	goto found;
    }

    LOCK(map);
    data = skiplist_insert(&map->list, &search, &is_new);
    SECURE(data->magic = ND_MAGIC);
    if ( is_new )
    { map->new = map->new*0.99+1.0;
      map->value_count++;
      lock_datum(search.data.key);
    } else
    { int rc;

    found:
      map->existing = map->existing*0.99+1.0;
      if ( (rc = insert_atom_set(map, &data->values, a2)) > 0 )
      { map->value_count++;
	lock_datum(a2);
      } else if ( rc < 0 )
      { UNLOCK(map);
	exit_scan(&map->defer);
	return PL_resource_error("memory");
      }
    }
    UNLOCK(map);
    if ( !is_new )
    { destroy_atom_set(&search.data.values);
    }
  }

  exit_scan(&map->defer);
  return TRUE;
}


static foreign_t
insert_atom_map3(term_t handle, term_t from, term_t to)
{ return insert_atom_map4(handle, from, to, 0);
}


		 /*******************************
		 *	       DELETE		*
		 *******************************/

static foreign_t
delete_atom_map2(term_t handle, term_t from)
{ atom_map *map;
  node_data_ex search;
  node_data *data;

  if ( !get_atom_map(handle, &map) ||
       !get_search_datum(from, &search) )
    return FALSE;

  LOCK(map);
  enter_scan(&map->defer);
  if ( (data = skiplist_delete(&map->list, &search)) )
  { map->value_count -= data->values.size;
    deferred_finalize(&map->defer, data, free_node_data, map);
  }
  exit_scan(&map->defer);
  UNLOCK(map);

  return TRUE;
}


static foreign_t
delete_atom_map3(term_t handle, term_t from, term_t to)
{ atom_map *map;
  node_data_ex search;
  node_data *data;
  datum a2;

  if ( !get_atom_map(handle, &map) ||
       !get_search_datum(from, &search) ||
       !get_datum(to, &a2) )
    return FALSE;

  enter_scan(&map->defer);
  if ( (data = skiplist_find(&map->list, &search)) &&
       in_atom_set(&data->values, a2) )
  { atom_set *as = &data->values;

    LOCK(map);
    if ( !skiplist_erased_payload(&map->list, data) &&
	 delete_atom_set(map, as, a2) )
    { unlock_datum(a2);
      map->value_count--;
      if ( as->size == 0 )
      { search.data = *data;
	if ( data != skiplist_delete(&map->list, &search) )
	  assert(0);
	deferred_finalize(&map->defer, data, free_node_data, map);
      }
    }
    UNLOCK(map);
  }
  exit_scan(&map->defer);

  return TRUE;
}


		 /*******************************
		 *	      SEARCH		*
		 *******************************/

typedef struct
{ atom_set *set;
  int      neg;				/* not(Lit) */
} pn_set;


static int
cmp_atom_set_size(const void *p1, const void *p2)
{ const pn_set *ap1 = p1;
  const pn_set *ap2 = p2;

  if ( ap1->neg != ap2->neg )
    return ap1->neg ? 1 : -1;		/* all negatives at the end */

  return ap1->set->size == ap2->set->size ? 0 :
         ap1->set->size < ap2->set->size ? -1 : 1;
}


#define MAX_SETS 100

static foreign_t
find_atom_map_protected(atom_map  *map, term_t keys, term_t literals)
{ pn_set as[MAX_SETS];				/* TBD */
  int ns = 0;
  term_t tmp = PL_new_term_ref();
  term_t tail = PL_copy_term_ref(keys);
  term_t head = PL_new_term_ref();
  atom_chash *ah;
  size_t ca;

  while(PL_get_list(tail, head, tail))
  { node_data *data;
    node_data_ex search;
    int neg = FALSE;

    if ( PL_is_functor(head, FUNCTOR_not1) )
    { _PL_get_arg(1, head, tmp);
      if ( !get_search_datum(tmp, &search) )
	return FALSE;
      neg = TRUE;
    } else
    { if ( !get_search_datum(head, &search) )
	return FALSE;
    }

    if ( (data = skiplist_find(&map->list, &search)) )
    { if ( ns+1 >= MAX_SETS )
	return PL_resource_error("max_search_atoms");

      as[ns].set = &data->values;
      as[ns].neg = neg;
      DEBUG(2, Sdprintf("Found atom-set of size %d\n", as[ns].set->size));
      ns++;
    } else if ( !neg )
    { return PL_unify_nil(literals);
    }
  }
  if ( !PL_get_nil(tail) )
    return PL_type_error("list", tail);

  qsort(as, ns, sizeof(*as), cmp_atom_set_size);
  if ( ns==0 || as[0].neg )
    return PL_domain_error("keywords", keys);

  PL_put_term(tail, literals);
  ah=as[0].set->entries;

  for(ca=0; ca<ah->allocated; ca++)
  { datum a = ah->atoms[ca];
    int i;

    if ( a == EMPTY )
      continue;

    for(i=1; i<ns; i++)
    { if ( !as[i].neg )
      { if ( !in_atom_set(as[i].set, a) )
	  goto next;
      } else
      { if ( in_atom_set(as[i].set, a) )
	  goto next;
      }
    }

    if ( !PL_unify_list(tail, head, tail) ||
	 !unify_datum(head, a) )
      return FALSE;
next:;
  }

  return PL_unify_nil(tail);
}


static foreign_t
find_atom_map(term_t handle, term_t keys, term_t literals)
{ atom_map *map;
  int rc;

  if ( !get_atom_map(handle, &map) )
    return FALSE;

  enter_scan(&map->defer);
  rc = find_atom_map_protected(map, keys, literals);
  exit_scan(&map->defer);

  return rc;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
rdf_keys_in_literal_map(+Map, +Spec, -Keys)

Spec is one of

	* all
	* prefix(Text)			atoms only
	* ge(Low)			integers only
	* le(High)
	* between(Low, High)
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
between_keys(atom_map *map, intptr_t min, intptr_t max, term_t head, term_t tail)
{ skiplist_enum state;
  node_data *data;
  node_data_ex search;

  DEBUG(2, Sdprintf("between %ld .. %ld\n", min, max));

  search.data.key = integer_to_datum(min);
  SECURE(search.magic = ND_MAGIC_EX);

  if ( (data = skiplist_find_first(&map->list, &search, &state)) &&
       isIntDatum(data->key) )
  { for(;;)
    { if ( integer_from_datum(data->key) > max )
	break;

      if ( !PL_unify_list(tail, head, tail) ||
	   !unify_datum(head, data->key) )
      { skiplist_find_destroy(&state);
	return FALSE;
      }

      if ( !(data = skiplist_find_next(&state)) ||
	   !isIntDatum(data->key) )
	break;
    }

    skiplist_find_destroy(&state);
  }

  return TRUE;
}


static foreign_t
rdf_keys_in_literal_map_proteced(atom_map *map, term_t spec, term_t keys)
{ term_t tail = PL_copy_term_ref(keys);
  term_t head = PL_new_term_ref();
  atom_t name;
  size_t arity;

  if ( !PL_get_name_arity(spec, &name, &arity) )
    PL_type_error("key-specifier", spec);

  if ( name == ATOM_all )
  { skiplist_enum state;
    node_data *data;

    for(data = skiplist_find_first(&map->list, NULL, &state);
	data;
	data=skiplist_find_next(&state))
    { if ( !PL_unify_list(tail, head, tail) ||
	   !unify_datum(head, data->key) )
      { skiplist_find_destroy(&state);
	return FALSE;
      }
    }
    skiplist_find_destroy(&state);
  } else if ( name == ATOM_key && arity == 1 )
  { term_t a = PL_new_term_ref();
    node_data *data;
    node_data_ex search;

    _PL_get_arg(1, spec, a);
    if ( !get_search_datum(a, &search) )
      return FALSE;

    if ( (data = skiplist_find(&map->list, &search)) )
    { intptr_t size = (intptr_t)data->values.size;

      if ( size > 0 )
	return PL_unify_integer(keys, size);
      assert(size == 0);
    }

    return FALSE;
  } else if ( (name == ATOM_prefix || name == ATOM_case) && arity == 1 )
  { term_t a = PL_new_term_ref();
    atom_t prefix, first_a;
    skiplist_enum state;
    node_data *data;
    node_data_ex search;
    int match = (name == ATOM_prefix ? STR_MATCH_PREFIX : STR_MATCH_ICASE);

    _PL_get_arg(1, spec, a);
    if ( !PL_get_atom_ex(a, &prefix) )
      return FALSE;
    first_a = first_atom(prefix, STR_MATCH_PREFIX);

    search.data.key = atom_to_datum(first_a);
    search.atom.handle = first_a;
    search.atom.resolved = FALSE;
    SECURE(search.magic = ND_MAGIC_EX);

    for(data = skiplist_find_first(&map->list, &search, &state);
	data;
	data=skiplist_find_next(&state))
    { assert(isAtomDatum(data->key));

      if ( !match_atoms(match,
			first_a, atom_from_datum(data->key)) )
	break;

      if ( !PL_unify_list(tail, head, tail) ||
	   !unify_datum(head, data->key) )
      { skiplist_find_destroy(&state);
	return FALSE;
      }

      skiplist_find_destroy(&state);
    }
  } else if ( (name == ATOM_ge || name == ATOM_le) && arity == 1 )
  { term_t a = PL_new_term_ref();
    intptr_t val, min, max;

    _PL_get_arg(1, spec, a);
    if ( !PL_get_intptr_ex(a, &val) )
      return FALSE;

    if ( name == ATOM_ge )
      min = val, max = MAP_MAX_INT;
    else
      max = val, min = MAP_MIN_INT;

    if ( !between_keys(map, min, max, head, tail) )
      return FALSE;
  } else if ( name == ATOM_between && arity == 2 )
  { term_t a = PL_new_term_ref();
    intptr_t min, max;

    _PL_get_arg(1, spec, a);
    if ( !PL_get_intptr_ex(a, &min) )
      return FALSE;
    _PL_get_arg(2, spec, a);
    if ( !PL_get_intptr_ex(a, &max) )
      return FALSE;

    if ( !between_keys(map, min, max, head, tail) )
      return FALSE;
  } else
  { return PL_type_error("key-specifier", spec);
  }

  return PL_unify_nil(tail);
}


static foreign_t
rdf_keys_in_literal_map(term_t handle, term_t spec, term_t keys)
{ atom_map *map;
  int rc;

  if ( !get_atom_map(handle, &map) )
    return FALSE;

  enter_scan(&map->defer);
  rc = rdf_keys_in_literal_map_proteced(map, spec, keys);
  exit_scan(&map->defer);

  return rc;
}


		 /*******************************
		 *	      RESET		*
		 *******************************/

static foreign_t
rdf_reset_literal_map(term_t handle)
{ atom_map *map;

  if ( !get_atom_map(handle, &map) )
    return FALSE;

  LOCK(map);
  skiplist_destroy(&map->list);
  init_map(map);
  map->value_count = 0;
  UNLOCK(map);

  return TRUE;
}



		 /*******************************
		 *	    STATISTICS		*
		 *******************************/


term_t
rdf_statistics_literal_map(term_t map, term_t key)
{ atom_map *m;

  if ( !get_atom_map(map, &m) )
    return FALSE;

  if ( PL_is_functor(key, FUNCTOR_size2) )
  { term_t a = PL_new_term_ref();

    _PL_get_arg(1, key, a);
    if ( !PL_unify_integer(a, m->list.count) )
      return FALSE;
    _PL_get_arg(2, key, a);

    return PL_unify_integer(a, m->value_count);
  }

  return PL_type_error("statistics_key", key);
}




		 /*******************************
		 *	     REGISTER		*
		 *******************************/

#define PRED(n,a,f,o) PL_register_foreign(n,a,f,o)

install_t
install_atom_map(void)
{ init_functors();
  init_datum_store();

  PRED("rdf_new_literal_map",	     1,	new_atom_map,		    0);
  PRED("rdf_destroy_literal_map",    1,	destroy_atom_map,	    0);
  PRED("rdf_reset_literal_map",	     1, rdf_reset_literal_map,	    0);
  PRED("rdf_insert_literal_map",     3,	insert_atom_map3,	    0);
  PRED("rdf_insert_literal_map",     4,	insert_atom_map4,	    0);
  PRED("rdf_delete_literal_map",     3,	delete_atom_map3,	    0);
  PRED("rdf_delete_literal_map",     2,	delete_atom_map2,	    0);
  PRED("rdf_find_literal_map",	     3,	find_atom_map,		    0);
  PRED("rdf_keys_in_literal_map",    3,	rdf_keys_in_literal_map,    0);
  PRED("rdf_statistics_literal_map", 2,	rdf_statistics_literal_map, 0);
}
