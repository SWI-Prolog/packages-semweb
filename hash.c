/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2007-2012, University of Amsterdam
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
