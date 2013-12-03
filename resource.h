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
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
    MA 02110-1301 USA
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
