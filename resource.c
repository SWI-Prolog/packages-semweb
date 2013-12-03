/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2002-2013, VU University Amsterdam

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

#include "rdf_db.h"
#include "murmur.h"

static int
init_resource_hash(resource_db *rdb)
{ size_t bytes = sizeof(resource**)*INITIAL_RESOURCE_TABLE_SIZE;
  resource **r = rdf_malloc(rdb->db, bytes);
  int i, count = INITIAL_RESOURCE_TABLE_SIZE;

  memset(r, 0, bytes);
  for(i=0; i<MSB(count); i++)
    rdb->hash.blocks[i] = r;

  rdb->hash.bucket_count       = count;
  rdb->hash.bucket_count_epoch = count;
  rdb->hash.count              = 0;

  return TRUE;
}


static void
free_resource_chain(rdf_db *db, resource *r)
{ resource *n;

  for(; r; r=n)
  { n = r->next;
    PL_unregister_atom(r->name);
    rdf_free(db, r, sizeof(*r));
  }
}

static void
free_resource_chains(rdf_db *db, resource **rl, int count)
{ int i;

  for(i=0; i<count; i++)
    free_resource_chain(db, rl[i]);

  rdf_free(db, rl, sizeof(resource**)*count);
}

static void
erase_resource_hash(resource_db *rdb)
{ if ( rdb->hash.blocks[0] )
  { int i, count = INITIAL_RESOURCE_TABLE_SIZE;

    free_resource_chains(rdb->db, rdb->hash.blocks[0], count);

    for(i=MSB(count); i<MAX_RBLOCKS; i++)
    { resource **r = rdb->hash.blocks[i];

      if ( r )
      { int size = BLOCKLEN(i);

	r += size;
	free_resource_chains(rdb->db, r, size);
      } else
	break;
    }
  }

  memset(&rdb->hash, 0, sizeof(rdb->hash));
}


static int
resize_resource_table(resource_db *rdb)
{ int i = MSB(rdb->hash.bucket_count);
  size_t bytes  = sizeof(resource**)*rdb->hash.bucket_count;
  resource **r = rdf_malloc(rdb->db, bytes);

  memset(r, 0, bytes);
  rdb->hash.blocks[i] = r-rdb->hash.bucket_count;
  rdb->hash.bucket_count *= 2;
  DEBUG(1, Sdprintf("Resized resource table to %ld\n",
		    (long)rdb->hash.bucket_count));

  return TRUE;
}


int
init_resource_db(rdf_db *db, resource_db *rdb)
{ rdb->db = db;
  init_resource_hash(rdb);

  return TRUE;
}


void
erase_resources(resource_db *rdb)
{ erase_resource_hash(rdb);
}


typedef struct res_walker
{ resource_db  *rdb;			/* Resource DB */
  atom_t	name;			/* Name of the resource */
  size_t	unbounded_hash;		/* Atom's hash */
  size_t	bcount;			/* current bucket count */
  resource     *current;		/* current location */
} res_walker;


static void
init_res_walker(res_walker *rw, resource_db *rdb, atom_t name)
{ rw->rdb	     = rdb;
  rw->name	     = name;
  rw->unbounded_hash = atom_hash(name, MURMUR_SEED);
  rw->bcount	     = rdb->hash.bucket_count_epoch;
  rw->current	     = NULL;
}


static resource*
next_resource(res_walker *rw)
{ resource *r;

  if ( rw->current )
  { r = rw->current;
    rw->current = r->next;
  } else if ( rw->bcount <= rw->rdb->hash.bucket_count )
  { do
    { int entry = rw->unbounded_hash % rw->bcount;
      r = rw->rdb->hash.blocks[MSB(entry)][entry];
      rw->bcount *= 2;
    } while(!r && rw->bcount <= rw->rdb->hash.bucket_count );

    if ( r )
      rw->current = r->next;
  } else
    return NULL;

  return r;
}


static resource *
existing_resource(resource_db *rdb, atom_t name)
{ res_walker rw;
  resource *r;

  init_res_walker(&rw, rdb, name);
  while((r=next_resource(&rw)))
  { if ( r->name == name )
      return r;
  }

  return NULL;
}


resource *
lookup_resource(resource_db *rdb, atom_t name)
{ resource *r, **rp;
  int entry;

  if ( (r=existing_resource(rdb, name)) )
    return r;

  LOCK_MISC(rdb->db);
  if ( (r=existing_resource(rdb, name)) )
  { UNLOCK_MISC(rdb->db);
    return r;
  }

  r = rdf_malloc(rdb->db, sizeof(*r));
  memset(r, 0, sizeof(*r));
  r->name = name;
  PL_register_atom(name);
  if ( rdb->hash.count > rdb->hash.bucket_count )
    resize_resource_table(rdb);
  entry = atom_hash(name, MURMUR_SEED) % rdb->hash.bucket_count;
  rp = &rdb->hash.blocks[MSB(entry)][entry];
  r->next = *rp;
  *rp = r;
  rdb->hash.count++;
  UNLOCK_MISC(rdb->db);

  return r;
}


resource *
register_resource(resource_db *rdb, atom_t name)
{ resource *r = lookup_resource(rdb, name);

  assert(r);
  ATOMIC_INC(&r->references);

  return r;
}


resource *
unregister_resource(resource_db *rdb, atom_t name)
{ resource *r = existing_resource(rdb, name);

  ATOMIC_DEC(&r->references);

  return r;
}



		 /*******************************
		 *	       PROLOG		*
		 *******************************/

typedef struct res_enum
{ resource_db *rdb;
  resource    *current;
  int	       current_entry;
} res_enum;


static foreign_t
rdf_resource(term_t r, control_t h)
{ rdf_db *db = rdf_current_db();
  res_enum *state;

  switch( PL_foreign_control(h) )
  { case PL_FIRST_CALL:
    { atom_t name;

      if ( PL_is_variable(r) )
      { state = PL_malloc_uncollectable(sizeof(*state));
	state->rdb = &db->resources;
	state->current = NULL;
	state->current_entry = -1;
	break;
      } else if ( PL_get_atom_ex(r, &name) )
      { resource *r;

	if ( (r=existing_resource(&db->resources, name)) &&
	     r->references > 0
	   )
	  return TRUE;
	return FALSE;
      }

      return FALSE;
    }
    case PL_REDO:
      state = PL_foreign_context_address(h);
      break;
    case PL_PRUNED:
      state = PL_foreign_context_address(h);
      rdf_free(db, state, sizeof(*state));
      return TRUE;
    default:
      assert(0);
      return FALSE;
  }

  for(;;)
  { int ce;

    for ( ; state->current; state->current = state->current->next )
    { if ( state->current->references )
      { if ( !PL_unify_atom(r, state->current->name) )
	{ PL_free(state);
	  return FALSE;				/* error */
	}
	state->current = state->current->next;
	PL_retry_address(state);
      }
    }

    if ( (ce = ++state->current_entry) < state->rdb->hash.bucket_count )
    { state->current = state->rdb->hash.blocks[MSB(ce)][ce];
    } else
    { PL_free(state);
      return FALSE;
    }
  }
}


#ifdef O_DEBUG
#define RDF_LOOKUP_RESOURCE
static foreign_t
rdf_lookup_resource(term_t r)
{ rdf_db *db = rdf_current_db();
  atom_t a;

  if ( !PL_get_atom_ex(r, &a) )
    return FALSE;

  lookup_resource(&db->resources, a);

  return TRUE;
}
#endif

#define NDET PL_FA_NONDETERMINISTIC

int
register_resource_predicates(void)
{ PL_register_foreign("rdf_resource",        1, rdf_resource,        NDET);
#ifdef RDF_LOOKUP_RESOURCE
  PL_register_foreign("rdf_lookup_resource", 1, rdf_lookup_resource, 0);
#endif

  return TRUE;
}
