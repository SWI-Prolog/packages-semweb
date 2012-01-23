/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2011, VU University Amsterdam

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

#ifndef RDF_QUERY_H_INCLUDED
#define RDF_QUERY_H_INCLUDED
#include <stdint.h>
#include "rdf_db.h"
#include "mutex.h"




typedef struct rdf_db *rdf_dbp;
typedef struct triple *triplep;


		 /*******************************
		 *	      QUERIES		*
		 *******************************/

typedef enum q_type
{ Q_NORMAL = 0,				/* Normal query */
  Q_TRANSACTION				/* A transaction */
} q_type;

typedef struct query
{ gen_t		rd_gen;			/* generation for reading */
  gen_t		wr_gen;			/* generation for writing */
  rdf_dbp	db;			/* Database on which we run */
  struct query *parent;			/* Parent query */
  struct query_stack  *stack;		/* Query-stack I am part of */
  q_type	type;			/* Q_* */
  int		depth;			/* recursion depth */
  struct query *transaction;		/* Transaction of the query */
  struct
  { gen_t	rd_gen_saved;
    gen_t	wr_gen_saved;
    struct triple_buffer *added;
    struct triple_buffer *deleted;
    term_t	prolog_id;		/* Prolog transaction identifier */
  } transaction_data;
  search_state	search_state;		/* State for searches */
} query;

#define MAX_QBLOCKS 20			/* allows for 2M concurrent queries */

typedef struct query_stack
{ query	       *blocks[MAX_QBLOCKS];
  query		preallocated[4];
  simpleMutex	lock;
  query	       *transaction;		/* Current transaction */
  gen_t		rd_gen;			/* generation for reading */
  gen_t		wr_gen;			/* generation for writing */
  rdf_dbp	db;			/* DB we are associated to */
  int		top;			/* Top of query stack */
} query_stack;


		 /*******************************
		 *	      THREADS		*
		 *******************************/

typedef struct thread_info
{ query_stack   queries;		/* Open queries */
} thread_info;

		 /*******************************
		 *		API		*
		 *******************************/

COMMON(void)	init_query_admin(rdf_dbp db);
COMMON(query *)	open_query(rdf_dbp db);
COMMON(void)	close_query(query *q);
COMMON(gen_t)	oldest_query_geneneration(rdf_db *db);

COMMON(query *)	open_transaction(rdf_dbp db,
				 struct triple_buffer *added,
				 struct triple_buffer *deleted);
COMMON(int)	empty_transaction(query *q);
COMMON(int)	commit_transaction(query *q);
COMMON(void)	close_transaction(query *q);
COMMON(int)	discard_transaction(query *q);

COMMON(int)	add_triples(query *q, triplep *triples, size_t count);
COMMON(int)	del_triples(query *q, triplep *triples, size_t count);
COMMON(int)	update_triples(query *q,
			       triplep *old, triplep *new, size_t count);
COMMON(int)	alive_triple(query *q, triplep t);

#endif /*RDF_QUERY_H_INCLUDED*/
