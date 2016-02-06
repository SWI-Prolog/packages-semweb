/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2011-2013, VU University Amsterdam
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

#ifndef RESOURCE_H_INCLUDED
#define RESOURCE_H_INCLUDED

#define MAX_RBLOCKS 32

typedef struct resource
{ atom_t	name;			/* identifier of the resource */
  struct resource *next;		/* Next in hash */
  size_t	references;		/* #times used */
} resource;

typedef struct resource_hash
{ resource    **blocks[MAX_RBLOCKS];	/* Dynamic array starts */
  size_t	bucket_count;		/* Allocated #buckets */
  size_t	bucket_count_epoch;	/* Initial bucket count */
  size_t	count;			/* Total #resources */
} resource_hash;

typedef struct resource_db
{ resource_hash	hash;			/* Hash atom-->id */
  struct rdf_db	*db;			/* RDF database I belong to */
} resource_db;

COMMON(int)	   init_resource_db(struct rdf_db *db, resource_db *rdb);
COMMON(void)	   erase_resources(resource_db *rdb);
COMMON(resource *) lookup_resource(resource_db *rdb, atom_t name);
COMMON(int)	   register_resource_predicates(void);
COMMON(resource *) register_resource(resource_db *rdb, atom_t name);
COMMON(resource *) unregister_resource(resource_db *rdb, atom_t name);

#endif /*RESOURCE_H_INCLUDED*/
