/*  $Id$

    Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2002-2010, VU University Amsterdam

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

#include "rdf_db.h"

static int
init_resource_hash(resource_db *rdb)
{ size_t bytes = sizeof(resource**)*INITIAL_RESOURCE_TABLE_SIZE;
  resource **r = rdf_malloc(rdb->db, bytes);
  int i, count = INITIAL_PREDICATE_TABLE_SIZE;

  memset(r, 0, bytes);
  for(i=0; i<MSB(count); i++)
    rdb->hash.blocks[i] = r;

  rdb->hash.bucket_count       = count;
  rdb->hash.bucket_count_epoch = count;
  rdb->hash.count              = 0;

  return TRUE;
}


static int
resize_resource_table(resource_db *rdb)
{ int i = MSB(rdb->hash.bucket_count);
  size_t bytes  = sizeof(resource**)*rdb->hash.bucket_count;
  resource **r = rdf_malloc(rdb->db, bytes);

  memset(r, 0, bytes);
  rdb->hash.blocks[i] = r-rdb->hash.bucket_count;
  rdb->hash.bucket_count *= 2;
  DEBUG(0, Sdprintf("Resized resource table to %ld\n",
		    (long)rdb->hash.bucket_count));

  return TRUE;
}


int
init_resource_db(rdf_db *db, resource_db *rdb)
{ rdb->db = db;
  init_resource_hash(rdb);

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    MSB	 IDs		Offset
     0	 0,1		0
     1   2,3		2
     2   4,5,6,7	4

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
set_id_resource(resource_db *rdb, resource *r)
{ size_t id;

  MUST_HOLD(rdb->db);

  for(id=rdb->array.first_free; ; id++)
  { int idx = MSB(id);
    resource **b = rdb->array.blocks[idx];

    if ( !b )
    { size_t count = id < 2 ? 2 : 1<<idx;
      size_t bytes = count*sizeof(resource*);

      b = rdf_malloc(rdb->db, bytes);
      memset(b, 0, bytes);
      if ( id >= 2 )
	b -= count;
      rdb->array.blocks[idx] = b;
    }

    if ( !b[id] )
    { b[id] = r;
      r->id = id;
      rdb->array.first_free = id+1;
      if ( rdb->array.highest_id < id )
	rdb->array.highest_id = id;

      return TRUE;
    }
  }
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
  rw->unbounded_hash = atom_hash(name);
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
  set_id_resource(rdb, r);
  PL_register_atom(name);
  if ( rdb->hash.count > rdb->hash.bucket_count )
    resize_resource_table(rdb);
  entry = atom_hash(name) % rdb->hash.bucket_count;
  rp = &rdb->hash.blocks[MSB(entry)][entry];
  r->next = *rp;
  *rp = r;
  rdb->hash.count++;
  DEBUG(5, Sdprintf("Resource %s (id = %ld)\n",
		    PL_atom_chars(name), (long)r->id));
  UNLOCK_MISC(rdb->db);

  return r;
}


		 /*******************************
		 *	       PROLOG		*
		 *******************************/

static foreign_t
rdf_resource(term_t r, control_t h)
{ rdf_db *db = DB;
  size_t id;

  switch( PL_foreign_control(h) )
  { case PL_FIRST_CALL:
    { atom_t name;

      if ( PL_is_variable(r) )
      { id = 0;
	break;
      } else if ( get_atom_ex(r, &name) )
      { if ( existing_resource(&db->resources, name) )
	  return TRUE;
	return FALSE;
      } else
	return FALSE;
    }
    case PL_REDO:
      id = PL_foreign_context(h);
      break;
    case PL_PRUNED:
      return TRUE;
  }

  for(; id<=db->resources.array.highest_id; id++)
  { resource **b = db->resources.array.blocks[MSB(id)];

    assert(b);
    if ( b[id] )
    { if ( !PL_unify_atom(r, (*b)->name) )
	return FALSE;			/* error */
      PL_retry(id);
    }
  }

  return FALSE;
}


static foreign_t
pl_lookup_resource(term_t name, term_t id)
{ rdf_db *db = DB;
  resource *r;
  atom_t a;

  if ( !get_atom_ex(name, &a) )
    return FALSE;

  r = lookup_resource(&db->resources, a);

  return PL_unify_int64(id, r->id);
}


#define NDET PL_FA_NONDETERMINISTIC

int
register_resource_predicates(void)
{ PL_register_foreign("rdf_resource",	     1, rdf_resource,       NDET);
  PL_register_foreign("rdf_lookup_resource", 2, pl_lookup_resource, 0);

  return TRUE;
}
