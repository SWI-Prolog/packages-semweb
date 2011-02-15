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


static int
set_id_resource(resource_db *rdb, resource *r)
{ size_t id;

  for(id=rdb->array.first_free; ; id++)
  { resource **b = rdb->array.blocks[MSB(id)];

    if ( !b )
    { LOCK_MISC(rdb->db);
      b = rdb->array.blocks[MSB(id)];
      if ( !b )
      { size_t count = 1<<MSB(id);
	size_t bytes = count*sizeof(resource*);
	resource **rp = rdf_malloc(rdb->db, bytes);

	memset(rp, 0, bytes);
	b = rdb->array.blocks[MSB(id)] = rp-count;
      }
      UNLOCK_MISC(rdb->db);
    }

    if ( !b[id] )
    { LOCK_MISC(rdb->db);
      if ( !b[id] )
      { b[id] = r;
	r->id = id;
	rdb->array.first_free = id+1;
	UNLOCK_MISC(rdb->db);
	return TRUE;
      }
      UNLOCK_MISC(rdb->db);
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


resource *
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


static resource *
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
