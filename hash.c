/*  $Id$

    Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        wielemak@science.uva.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 1985-2007, University of Amsterdam

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

    As a special exception, if you link this library with other files,
    compiled with a Free Software compiler, to produce an executable, this
    library does not by itself cause the resulting executable to be covered
    by the GNU General Public License. This exception does not however
    invalidate any other reasons why the executable file might be covered by
    the GNU General Public License.
*/

#include "rdf_db.h"
#include "hash.h"
#include "murmur.h"
#include <SWI-Prolog.h>
#include <string.h>

static unsigned int
ptr_hash_key(void *ptr)
{ return rdf_murmer_hash(&ptr, sizeof(ptr), MURMUR_SEED);
}


ptr_hash_table *
new_ptr_hash(int entries)
{ ptr_hash_table *hash = PL_malloc(sizeof(*hash));
  size_t size = sizeof(*hash->chains)*entries;

  memset(hash, 0, sizeof(*hash));
  hash->entries = entries;
  hash->chains  = PL_malloc(size);
  memset(hash->chains, 0, size);

  return hash;
}


static int
destroy_node(ptr_hash_node *node, void *closure)
{ PL_free(node);

  return TRUE;
}


void
destroy_ptr_hash(ptr_hash_table *hash)
{ for_ptr_hash(hash, destroy_node, NULL);

  PL_free(hash->chains);
  PL_free(hash);
}


int
add_ptr_hash(ptr_hash_table *hash, void *value)
{ int key = ptr_hash_key(value)%hash->entries;
  ptr_hash_node *node;

  for(node = hash->chains[key]; node; node = node->next)
  { if ( node->value == value )
      return FALSE;			/* already in hash */
  }

  node = PL_malloc(sizeof(*node));
  node->value = value;
  node->next = hash->chains[key];
  hash->chains[key] = node;

  return TRUE;
}


int
for_ptr_hash(ptr_hash_table *hash,
	     int (*func)(ptr_hash_node *node, void *closure),
	     void *closure)
{ int key;

  for(key=0; key < hash->entries; key++)
  { ptr_hash_node *node;
    ptr_hash_node *next;

    for(node=hash->chains[key]; node; node = next)
    { next = node->next;

      if ( !func(node, closure) )
	return FALSE;
    }
  }

  return TRUE;
}
