/*  $Id$

    Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2002-2011, University of Amsterdam
			      VU University Amsterdam

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
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/

#ifndef RDFDB_H_INCLUDED
#define RDFDB_H_INCLUDED
#include "atom.h"

		 /*******************************
		 *	     OPTIONS		*
		 *******************************/

#define WITH_MD5 1

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Symbols are local to shared objects  by   default  in  COFF based binary
formats, and public in ELF based formats.   In some ELF based systems it
is possible to make them local   anyway. This enhances encapsulation and
avoids an indirection for calling these   functions.  Functions that are
supposed to be local to the SWI-Prolog kernel are declared using

    COMMON(<type) <function>(<args>);
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#ifdef HAVE_VISIBILITY_ATTRIBUTE
#define SO_LOCAL __attribute__((visibility("hidden")))
#else
#define SO_LOCAL
#endif
#define COMMON(type) SO_LOCAL type

		 /*******************************
		 *	   OTHER MODULES	*
		 *******************************/

#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <assert.h>
#include <string.h>
#include "debug.h"
#include "memory.h"
#include "hash.h"
#include "error.h"
#include "skiplist.h"
#ifdef WITH_MD5
#include "md5.h"
#endif
#include "mutex.h"
#include "resource.h"

#define RDF_VERSION 30000		/* 3.0.0 */

#define URL_subPropertyOf \
	"http://www.w3.org/2000/01/rdf-schema#subPropertyOf"


		 /*******************************
		 *	       LOCKING		*
		 *******************************/

#define MUST_HOLD(lock)			((void)0)
#define LOCK_MISC(db)			simpleMutexLock(&db->locks.misc)
#define UNLOCK_MISC(db)			simpleMutexUnlock(&db->locks.misc)


		 /*******************************
		 *	    GENERATIONS		*
		 *******************************/

typedef uint64_t gen_t;			/* Basic type for generations */

typedef struct lifespan
{ gen_t		born;			/* Generation we were born */
  gen_t		died;			/* Generation we died */
} lifespan;

#define GEN_UNDEF	0xffffffffffffffff /* no defined generation */
#define GEN_MAX		0x7fffffffffffffff /* Max `normal' generation */
#define GEN_PREHIST	0x0000000000000000 /* The generation epoch */
#define GEN_EPOCH	0x0000000000000001 /* The generation epoch */
#define GEN_TBASE	0x8000000000000000 /* Transaction generation base */
#define GEN_TNEST	0x0000000100000000 /* Max transaction nesting */

					/* Generation of a transaction */
#define T_GEN(tid,d)	(GEN_TBASE + (tid)*GEN_TNEST + (d))

#include "snapshot.h"


		 /*******************************
		 *	       TRIPLES		*
		 *******************************/

/* Keep consistent with md5_type[] in rdf_db.c! */
#define OBJ_UNTYPED	0x0		/* partial: don't know */
#define OBJ_INTEGER	0x1
#define OBJ_DOUBLE	0x2
#define OBJ_STRING	0x3
#define OBJ_TERM	0x4

#define Q_NONE		0x0
#define Q_TYPE		0x1
#define Q_LANG		0x2

#define BY_NONE	0x00			/* 0 */
#define BY_S	0x01			/* 1 */
#define BY_P	0x02			/* 2 */
#define BY_O	0x04			/* 4 */
#define BY_G	0x08			/* 8 */
#define BY_SP	(BY_S|BY_P)		/* 3 */
#define BY_SO	(BY_S|BY_O)		/* 5 */
#define BY_PO	(BY_P|BY_O)		/* 6 */
#define BY_SPO	(BY_S|BY_P|BY_O)	/* 7 */
#define BY_SG	(BY_S|BY_G)		/* 9 */
#define BY_PG	(BY_P|BY_G)		/* 10 */
#define BY_SPG	(BY_S|BY_P|BY_G)	/* 11 */
#define BY_OG	(BY_O|BY_G)		/* 12 */
#define BY_SOG	(BY_S|BY_O|BY_G)	/* 13 */
#define BY_POG	(BY_P|BY_O|BY_G)	/* 14 */
#define BY_SPOG	(BY_S|BY_P|BY_O|BY_G)	/* 15 */

/* (*) INDEX_TABLES must be consistent with index_col[] in rdf_db.c */
#define INDEX_TABLES		        10	/* (*)  */
#define INITIAL_TABLE_SIZE		1024
#define INITIAL_RESOURCE_TABLE_SIZE	8192
#define INITIAL_PREDICATE_TABLE_SIZE	64
#define INITIAL_GRAPH_TABLE_SIZE	64

#define MAX_HASH_FACTOR 8		/* factor to trigger re-hash */
#define MIN_HASH_FACTOR 4		/* factor after re-hash */

#define NO_LINE	(0)

typedef struct cell
{ void *	value;			/* represented resource */
  struct cell  *next;			/* next in chain */
} cell;


typedef struct list
{ cell *head;				/* first in list */
  cell *tail;				/* tail of list */
} list;


typedef struct bitmatrix
{ size_t width;
  size_t heigth;
  int bits[1];
} bitmatrix;


#define DISTINCT_DIRECT 0		/* for ->distinct_subjects, etc */
#define DISTINCT_SUB    1

typedef struct predicate
{ atom_t	    name;		/* name of the predicate */
  struct predicate *next;		/* next in hash-table */
					/* hierarchy */
  list	            subPropertyOf;	/* the one I'm subPropertyOf */
  list	            siblings;		/* reverse of subPropertyOf */
  int		    label;		/* Numeric label in cloud */
  struct predicate_cloud *cloud;	/* cloud I belong to */
  size_t	    hash;		/* key used for hashing
					   (=hash if ->cloud is up-to-date) */
					/* properties */
  struct predicate *inverse_of;		/* my inverse predicate */
  unsigned	    transitive : 1;	/* P(a,b)&P(b,c) --> P(a,c) */
					/* statistics */
  size_t	    triple_count;	/* # triples on this predicate */
  size_t	    distinct_updated[2];/* Is count still valid? */
  size_t	    distinct_count[2];  /* Triple count at last update */
  size_t	    distinct_subjects[2];/* # distinct subject values */
  size_t	    distinct_objects[2];/* # distinct object values */
} predicate;

#define MAX_PBLOCKS 32

typedef struct pred_hash
{ predicate   **blocks[MAX_PBLOCKS];	/* Dynamic array starts */
  size_t	bucket_count;		/* Allocated #buckets */
  size_t	bucket_count_epoch;	/* Initial bucket count */
  size_t	count;			/* Total #predicates */
} pred_hash;


typedef struct sub_p_matrix
{ struct sub_p_matrix *older;		/* Reachability for older gen */
  lifespan      lifespan;		/* Lifespan this matrix is valid */
  bitmatrix    *matrix;			/* Actual reachability matrix */
} sub_p_matrix;


typedef struct predicate_cloud
{ struct predicate_cloud *merged_into;	/* Cloud was merged into target */
  sub_p_matrix *reachable;		/* cloud reachability matrices */
  predicate   **members;		/* member predicates */
  size_t	size;			/* size of the cloud */
  size_t	deleted;		/* See erase_predicates() */
  size_t	alt_hash_count;		/* Alternative hashes */
  unsigned int *alt_hashes;
  unsigned int  hash;			/* hash-code */
} predicate_cloud;


typedef struct graph
{ struct graph    *next;		/* next in table */
  atom_t	    name;		/* name of the graph */
  atom_t	    source;		/* URL graph was loaded from */
  double	    modified;		/* Modified time of source URL */
  int		    triple_count;	/* # triples associated to it */
#ifdef WITH_MD5
  unsigned	    md5 : 1;		/* do/don't record MD5 */
  md5_byte_t	    digest[16];		/* MD5 digest */
#endif
} graph;

#define MAX_GBLOCKS 32

typedef struct graph_hash
{ graph	      **blocks[MAX_GBLOCKS];	/* Dynamic array starts */
  size_t	bucket_count;		/* Allocated #buckets */
  size_t	bucket_count_epoch;	/* Initial bucket count */
  size_t	count;			/* Total #predicates */
} graph_hash;

typedef struct literal
{ union
  { atom_t	string;
    int64_t	integer;
    double	real;
    struct
    { record_t  record;
      size_t	len;
    } term;				/* external record */
  } value;
  atom_t	type_or_lang;		/* Type or language for literals */
  unsigned int  hash;			/* saved hash */
  unsigned	objtype : 3;
  unsigned	qualifier : 2;		/* Lang/Type qualifier */
  unsigned	shared : 1;		/* member of shared table */
  unsigned	term_loaded : 1;	/* OBJ_TERM from quick save file */
  unsigned	atoms_locked : 1;	/* Atoms have been locked */
  unsigned	references : 24;	/* # references to me */
} literal;


#define t_match next[0]

typedef struct triple
{ atom_t	subject;
  union
  { predicate*	r;			/* resolved: normal DB */
    atom_t	u;			/* used by rdf_load_db_/3 */
  } predicate;
  union
  { literal *	literal;
    atom_t	resource;
  } object;
  atom_t	graph;			/* where it comes from */
  lifespan	lifespan;		/* Start and end generation */
					/* indexing */
  union
  { struct triple*next[INDEX_TABLES];	/* hash-table next links */
    literal	end;			/* end for between(X,Y) patterns */
  } tp;					/* triple or pattern */
					/* smaller objects (e.g., flags) */
  uint32_t      line;			/* graph-line number */
  unsigned	object_is_literal : 1;	/* Object is a literal */
  unsigned	resolve_pred : 1;	/* predicates needs to be resolved */
  unsigned	indexed : 4;		/* Partials: BY_* */
  unsigned	match   : 4;		/* How to match literals */
  unsigned	inversed : 1;		/* Partials: using inverse match */
  unsigned	is_duplicate : 1;	/* I'm a duplicate */
  unsigned	allocated : 1;		/* Triple is allocated */
  unsigned	atoms_locked : 1;	/* Atoms have been locked */
  unsigned	linked : 4;		/* Linked into the hash-chains */
  unsigned	reindexed : 1;		/* Remapped by optimize_triple_hash() */
					/* Total: 32 */
} triple;


typedef enum
{ TR_MARK,				/* mark start for nesting */
  TR_SUB_START,				/* start nested transaction */
  TR_SUB_END,				/* end nested transaction */
  TR_ASSERT,				/* rdf_assert */
  TR_RETRACT,				/* rdf_retractall */
  TR_UPDATE,				/* rdf_update */
  TR_UPDATE_MD5,			/* update md5 src */
  TR_RESET,				/* rdf_reset_db */
  TR_VOID				/* no-op */
} tr_type;


typedef struct transaction_record
{ struct transaction_record    *previous;
  struct transaction_record    *next;
  tr_type			type;
  triple		       *triple;		/* new/deleted triple */
  union
  { triple		       *triple;		/* used for update */
    struct
    { atom_t			atom;
      unsigned long		line;
    } src;
    struct
    { graph		       *graph;
      md5_byte_t	       *digest;
    } md5;
    record_t		       transaction_id;
  } update;
} transaction_record;


typedef struct active_transaction
{ struct active_transaction *parent;
  term_t id;
} active_transaction;


typedef struct triple_bucket
{ triple       *head;			/* head of triple-list */
  triple       *tail;			/* Tail of triple-list */
  unsigned int	count;			/* #Triples in bucket */
} triple_bucket;

#define MAX_TBLOCKS 32

typedef struct triple_hash
{ triple_bucket	*blocks[MAX_TBLOCKS];	/* Dynamic array starts */
  size_t	bucket_count;		/* Allocated #buckets */
  size_t	bucket_count_epoch;	/* Initial bucket count */
} triple_hash;

typedef struct triple_walker
{ size_t	unbounded_hash;		/* The unbounded hash-key */
  int		icol;			/* index column */
  size_t	bcount;			/* Current bucket count */
  triple_hash  *hash;			/* The hash */
  triple       *current;		/* Our current location */
} triple_walker;

#define MAX_BLOCKS 20			/* allows for 2M threads */

typedef struct per_thread
{ struct thread_info **blocks[MAX_BLOCKS];
} per_thread;

typedef struct query_admin
{ gen_t		generation;		/* Global heart-beat */
  struct
  { simpleMutex	lock;			/* used to lock creation of per_thread */
    per_thread	per_thread;
    int		thread_max;		/* highest thread seen  */
  } query;				/* active query administration */
  struct
  { simpleMutex	lock;			/* Locks writing triples */
  } write;				/* write administration */
} query_admin;


typedef struct rdf_db
{ triple_bucket by_none;		/* Plain linked list of triples */
  triple_hash   hash[INDEX_TABLES];	/* Hash-tables */
  size_t	created;		/* #triples created */
  size_t	duplicates;		/* #duplicate triples */
  size_t	erased;			/* #triples erased */
  size_t	reindexed;		/* #triples reindexed (gc_hash_chain) */
  size_t	indexed[16];		/* Count calls (2**4 possible indices) */
  resource_db	resources;		/* admin of used resources */
  pred_hash	predicates;		/* Predicate table */
  size_t	agenda_created;		/* #visited nodes in agenda */
  graph_hash    graphs;			/* Graph table */
  graph	       *last_graph;		/* last accessed graph */
  query_admin	queries;		/* Active query administration */

  int		resetting;		/* We are in rdf_reset_db() */

  struct
  { int		count;			/* # garbage collections */
    int		busy;			/* Processing a GC */
    double	time;			/* time spent in GC */
    size_t	reclaimed_triples;	/* # reclaimed triples */
    size_t	reclaimed_reindexed;	/* # reclaimed reindexed triples */
    gen_t	last_gen;		/* Keep generation at last-GC */
  } gc;

  struct
  { simpleMutex	literal;		/* threaded access to literals */
    simpleMutex misc;			/* general DB locks */
    simpleMutex gc;			/* DB garbage collection lock */
  } locks;

  struct
  { snapshot *head;			/* head and tail of snapshot list */
    snapshot *tail;
    gen_t     keep;			/* generation to keep */
  } snapshots;

  skiplist      literals;		/* (shared) literals */
} rdf_db;


		 /*******************************
		 *	    QUERY TYPES		*
		 *******************************/

#define LITERAL_EX_MAGIC 0x2b97e881

typedef struct literal_ex
{ literal  *literal;			/* the real literal */
  atom_info atom;			/* prepared info on value */
#ifdef O_SECURE
  long	    magic;
#endif
} literal_ex;


typedef struct search_state
{ rdf_db       *db;			/* our database */
  term_t	subject;		/* Prolog term references */
  term_t	object;
  term_t	predicate;
  term_t	src;
  term_t	realpred;
  unsigned	allocated;		/* State has been allocated */
  unsigned	flags;			/* Misc flags controlling search */
  atom_t	prefix;			/* prefix and like search */
  int		has_literal_state;	/* Literal state is present */
  skiplist_enum literal_state;		/* Literal search state */
  literal      *literal_cursor;		/* pointer in current literal */
  literal_ex    lit_ex;			/* extended literal for fast compare */
  literal      *restart_lit;		/* for restarting literal search */
  skiplist_enum restart_lit_state;	/* for restarting literal search */
  triple_walker cursor;			/* Pointer in triple DB */
  struct query *query;			/* Associated query */
  predicate_cloud *p_cloud;		/* Searched predicate cloud */
  int		alt_hash_cursor;	/* Index in alternative hashes */
  triple	pattern;		/* Pattern triple */
  triple	saved_pattern;		/* For inverse */
} search_state;

#include "query.h"

		 /*******************************
		 *	      FUNCTIONS		*
		 *******************************/

COMMON(void *)	rdf_malloc(rdf_db *db, size_t size);
COMMON(void)	rdf_free(rdf_db *db, void *ptr, size_t size);
COMMON(void *)	rdf_realloc(rdf_db *db, void *ptr, size_t old, size_t new);
COMMON(int)	link_triple(rdf_db *db, triple *t, query *q);
COMMON(void)	erase_triple(rdf_db *db, triple *t, query *q);
COMMON(predicate *) lookup_predicate(rdf_db *db, atom_t name, query *q);
COMMON(rdf_db*)	rdf_current_db(void);


#endif /*RDFDB_H_INCLUDED*/
