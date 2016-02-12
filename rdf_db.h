/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2003-2016, University of Amsterdam
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

#ifndef RDFDB_H_INCLUDED
#define RDFDB_H_INCLUDED
#include <config.h>

		 /*******************************
		 *	     OPTIONS		*
		 *******************************/

#define WITH_MD5 1
#if SIZEOF_VOIDP > 6
#define COMPACT  1
#endif

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
#include "atom.h"
#include "deferfree.h"
#include "debug.h"
#include "memory.h"
#include "hash.h"
#include "skiplist.h"
#ifdef WITH_MD5
#include "md5.h"
#endif
#include "mutex.h"
#include "resource.h"
#include "xsd.h"

#define RDF_VERSION 30000		/* 3.0.0 */

#define URL_subPropertyOf "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"


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
#define GEN_PREHIST	0x0000000000000000 /* The prehistoric generation */
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

#define DUPLICATE_ADMIN_THRESHOLD	1024

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


typedef struct is_leaf
{ struct is_leaf *older;		/* Older is_leaf info */
  lifespan	lifespan;		/* Computed for this lifespan */
  int		is_leaf;		/* Predicate was a leaf then */
} is_leaf;

#define DISTINCT_DIRECT 0		/* for ->distinct_subjects, etc */
#define DISTINCT_SUB    1

typedef struct predicate
{ atom_t	    name;		/* name of the predicate */
  struct predicate *next;		/* next in hash-table */
					/* hierarchy */
  list	            subPropertyOf;	/* the one I'm subPropertyOf */
  list	            siblings;		/* reverse of subPropertyOf */
  struct predicate_cloud *cloud;	/* cloud I belong to */
  is_leaf	   *is_leaf;		/* cached is-leaf information */
  struct predicate *inverse_of;		/* my inverse predicate */
  unsigned int	    hash;		/* key used for hashing */
  unsigned int	    label : 24;		/* Numeric label in cloud */
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
  int		last_gc;		/* number of last gc */
} predicate_cloud;


typedef struct graph
{ struct graph *next;			/* next in table */
  atom_t	name;			/* name of the graph */
  atom_t	source;			/* URL graph was loaded from */
  double	modified;		/* Modified time of source URL */
  int		triple_count;		/* # triples associated to it */
  unsigned	erased;			/* Graph is destroyed */
#ifdef WITH_MD5
  unsigned	md5 : 1;		/* do/don't record MD5 */
  md5_byte_t	digest[16];		/* MD5 digest */
  md5_byte_t	unmodified_digest[16];	/* MD5 digest when unmodified */
#endif
} graph;

#define MAX_GBLOCKS 32

typedef struct graph_hash_table
{ graph	      **blocks[MAX_GBLOCKS];	/* Dynamic array starts */
  size_t	bucket_count;		/* Allocated #buckets */
  size_t	bucket_count_epoch;	/* Initial bucket count */
  size_t	count;			/* Total #predicates */
  size_t	erased;			/* erased and not reclaimed graphs */
} graph_hash_table;

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
  atom_id	type_or_lang;		/* Type or language for literals */
  unsigned int  hash;			/* saved hash */
  unsigned int	references;		/* # references to me */
  unsigned	objtype : 3;
  unsigned	qualifier : 2;		/* Lang/Type qualifier */
  unsigned	shared : 1;		/* member of shared table */
  unsigned	term_loaded : 1;	/* OBJ_TERM from quick save file */
  unsigned	atoms_locked : 1;	/* Atoms have been locked */
} literal;


#define t_match next[0]

#ifdef COMPACT
#define TRIPLE_NO_ID	(0)
typedef unsigned int triple_id;		/* Triple identifier */
#endif

#ifdef TRIPLE_MAGIC

typedef enum
{ T_CHAINED1 = TRIPLE_MAGIC,		/* Added to hash-chains */
  T_CHAINED2,				/* Added to hash as reindexed */
  T_REINDEXED,				/* Reindexed (waiting for GC) */
  T_LINGERING,				/* waiting to be freed */
  T_FREED				/* freed */
} triple_status;

#define TMAGIC(t, s) (t->magic = s)

#else

#define TMAGIC(t, s) (void)0

#endif

typedef struct triple
{ lifespan	lifespan;		/* Start and end generation */
  atom_id	subject_id;
  atom_id	graph_id;		/* where it comes from */
  union
  { predicate*	r;			/* resolved: normal DB */
    atom_t	u;			/* used by rdf_load_db_/3 */
  } predicate;
  union
  { literal *	literal;
    atom_t	resource;
  } object;
#ifdef COMPACT
  triple_id	id;			/* Indentifier number */
  triple_id	reindexed;		/* Remapped by optimize_triple_hash() */
#else
  struct triple *reindexed;		/* Remapped by optimize_triple_hash() */
#endif
					/* indexing */
  union
  { literal	end;			/* end for between(X,Y) patterns */
#ifdef COMPACT
    triple_id	next[INDEX_TABLES];	/* hash-table next identifier */
#else
    struct triple*next[INDEX_TABLES];	/* hash-table next links */
#endif
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
  unsigned	loaded : 1;		/* for EV_ASSERT_LOAD */
  unsigned	erased : 1;		/* Consistency of erased */
  unsigned	lingering : 1;		/* Deleted; waiting for GC */
  unsigned	has_reindex_prev : 1;	/* some ->reindexed points to me */
					/* Total: 32 */
#ifdef TRIPLE_MAGIC
  triple_status	magic;
#endif
} triple;


typedef struct active_transaction
{ struct active_transaction *parent;
  term_t id;
} active_transaction;


typedef struct triple_bucket
{
#ifdef COMPACT
  triple_id     head;			/* head of triple-list */
  triple_id     tail;			/* Tail of triple-list */
#else
  triple       *head;			/* head of triple-list */
  triple       *tail;			/* Tail of triple-list */
#endif
  unsigned int	count;			/* #Triples in bucket */
} triple_bucket;


#define MAX_TBLOCKS 32

#ifdef COMPACT
#define TRIPLE_ARRAY_PREINIT 512

typedef union triple_element
{ triple *triple;			/* points to a triple */
  union triple_element *fnext;		/* poins to another free */
} triple_element;

typedef struct triple_array
{ triple_element *blocks[MAX_TBLOCKS];	/* Dynamic array starts */
  triple_element *freelist;		/* free buckets */
  size_t	preinit;		/* Preinitialized size */
  size_t	size;			/* current (allocated) size */
} triple_array;
#endif /*COMPACT*/

typedef struct triple_hash
{ triple_bucket	*blocks[MAX_TBLOCKS];	/* Dynamic array starts */
  size_t	bucket_count;		/* Allocated #buckets */
  size_t	bucket_count_epoch;	/* Initial bucket count */
  size_t	bucket_preinit;		/* Pre-initializaed bucket count */
  int		created;		/* Hash has been initialized */
  int		icol;			/* ICOL of hash */
  unsigned int	user_size;		/* User selected size as 2^N */
  unsigned int	optimize_threshold;	/* # resizes to leave behind */
  unsigned int	avg_chain_len;		/* Accepted average chain length */
} triple_hash;

typedef struct triple_walker
{ size_t	unbounded_hash;		/* The unbounded hash-key */
  int		icol;			/* index column */
  size_t	bcount;			/* Current bucket count */
  triple       *current;		/* Our current location */
  struct rdf_db *db;			/* the array of triples */
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
    simpleMutex generation_lock;	/* Interlocked fix of generations */
  } write;				/* write administration */
} query_admin;


#define PREFIX_INITIAL_ENTRIES 16

typedef struct prefix
{ atom_t	 alias;
  atom_info      uri;
  struct prefix *next;
} prefix;

typedef struct prefix_table
{ prefix       **entries;
  size_t	 size;
  size_t	 count;
} prefix_table;

#define JOINED_DEFER 1

#ifdef JOINED_DEFER
#define defer_triples  defer_all	/* Use the same for now */
#define defer_clouds   defer_all
#define defer_literals defer_all
#endif

typedef struct rdf_db
{ triple_bucket by_none;		/* Plain linked list of triples */
  triple_hash   hash[INDEX_TABLES];	/* Hash-tables */
#ifdef COMPACT
  triple_array	triple_array;		/* Dynamic array of triples */
#endif
  size_t	created;		/* #triples created */
  size_t	erased;			/* #triples erased */
  size_t	reindexed;		/* #triples reindexed (gc_hash_chain) */
  size_t	lingering;		/* #triples lingering to be freed */
  size_t	indexed[16];		/* Count calls (2**4 possible indices) */
  resource_db	resources;		/* admin of used resources */
  pred_hash	predicates;		/* Predicate table */
  size_t	agenda_created;		/* #visited nodes in agenda */
  graph_hash_table graphs;		/* Graph table */
  graph	       *last_graph;		/* last accessed graph */
  prefix_table *prefixes;		/* alias->uri table */
  query_admin	queries;		/* Active query administration */

  size_t	duplicates;		/* #duplicate triples */
  int		maintain_duplicates;	/* If TRUE, updated duplicates */
  int		duplicates_up_to_date;	/* Duplicate status is up-to-date */
  size_t	duplicate_admin_threshold;	/* Maintain above this */

#if JOINED_DEFER			/* Deferred free handling */
  defer_free	defer_all;
#else
  defer_free	defer_triples;		/* triples */
  defer_free	defer_clouds;		/* Predicate clouds */
  defer_free	defer_literals;		/* Literals */
#endif

  int		resetting;		/* We are in rdf_reset_db() */

  struct
  { int		count;			/* # garbage collections */
    int		busy;			/* Processing a GC */
    int		thread_started;		/* GC thread has been started */
    double	time;			/* time spent in GC */
    size_t	reclaimed_triples;	/* # reclaimed triples */
    size_t	reclaimed_reindexed;	/* # reclaimed reindexed triples */
    size_t	uncollectable;		/* # uncollectable erased at last GC */
    gen_t	last_gen;		/* Oldest generation at last-GC */
    gen_t	last_reindex_gen;	/* Oldest reindexed at last GC */
  } gc;

  struct
  { simpleMutex	literal;		/* threaded access to literals */
    simpleMutex misc;			/* general DB locks */
    simpleMutex gc;			/* DB garbage collection lock */
    simpleMutex duplicates;		/* Duplicate init lock */
    simpleMutex erase;			/* Erase triples */
    simpleMutex prefixes;		/* Prefix expansion */
  } locks;

  struct
  { snapshot *head;			/* head and tail of snapshot list */
    snapshot *tail;
    gen_t     keep;			/* generation to keep */
  } snapshots;

  skiplist      literals;		/* (shared) literals */
} rdf_db;


		 /*******************************
		 *	       SETS		*
		 *******************************/

#define CHUNKSIZE 4000				/* normally a page */

typedef struct mchunk
{ struct mchunk *next;
  size_t used;
  char buf[CHUNKSIZE];
} mchunk;

typedef struct tmp_store
{ mchunk     *chunks;
  mchunk      store0;
} tmp_store;


		 /*******************************
		 *	     TRIPLE SET		*
		 *******************************/

#define TRIPLESET_INITIAL_ENTRIES 4	/* often small */

typedef struct triple_cell
{ struct triple_cell *next;
  triple *triple;
} triple_cell;

typedef struct
{ triple_cell **entries;		/* Hash entries */
  size_t      size;			/* Hash-table size */
  size_t      count;			/* # atoms stored */
  tmp_store   store;			/* temporary store */
  triple_cell *entries0[TRIPLESET_INITIAL_ENTRIES];
} tripleset;


		 /*******************************
		 *	    QUERY TYPES		*
		 *******************************/

#include "buffer.h"

#define LITERAL_EX_MAGIC 0x2b97e881

typedef struct literal_ex
{ literal  *literal;			/* the real literal */
  atom_info atom;			/* prepared info on value */
#ifdef LITERAL_EX_MAGIC
  long	    magic;
#endif
} literal_ex;


typedef struct search_state
{ struct query *query;			/* Associated query */
  rdf_db       *db;			/* our database */
  term_t	subject;		/* Prolog term references */
  term_t	object;
  term_t	predicate;
  term_t	src;
  term_t	realpred;
  unsigned	flags;			/* Misc flags controlling search */
					/* START memset() cleared area */
  triple_walker cursor;			/* Pointer in triple DB */
  triple	pattern;		/* Pattern triple */
  atom_t	prefix;			/* prefix and like search */
  int		alt_hash_cursor;	/* Index in alternative hashes */
  int		has_literal_state;	/* Literal state is present */
  literal      *literal_cursor;		/* pointer in current literal */
  literal      *restart_lit;		/* for restarting literal search */
  skiplist_enum literal_state;		/* Literal search state */
  skiplist_enum restart_lit_state;	/* for restarting literal search */
  predicate_cloud *p_cloud;		/* Searched predicate cloud */
  triple       *prefetched;		/* Prefetched triple (retry) */
					/* END memset() cleared area */
  literal_ex    lit_ex;			/* extended literal for fast compare */
  tripleset	dup_answers;		/* possible duplicate answers */
} search_state;

#include "query.h"


		 /*******************************
		 *	      BROADCASTS	*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
The ids form a mask. This must be kept consistent with monitor_mask/2 in
rdf_db.pl!
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

typedef enum
{ EV_ASSERT      = 0x0001,		/* triple */
  EV_ASSERT_LOAD = 0x0002,		/* triple */
  EV_RETRACT     = 0x0004,		/* triple */
  EV_UPDATE      = 0x0008,		/* old, new */
  EV_NEW_LITERAL = 0x0010,		/* literal */
  EV_OLD_LITERAL = 0x0020,		/* literal */
  EV_TRANSACTION = 0x0040,		/* id, begin/end */
  EV_LOAD	 = 0x0080,		/* id, begin/end */
  EV_CREATE_GRAPH= 0x0100,		/* graph name */
  EV_RESET	 = 0x0200		/* reset */
} broadcast_id;


		 /*******************************
		 *	      FUNCTIONS		*
		 *******************************/

COMMON(void *)	rdf_malloc(rdf_db *db, size_t size);
COMMON(void)	rdf_free(rdf_db *db, void *ptr, size_t size);
COMMON(int)	prelink_triple(rdf_db *db, triple *t, query *q);
COMMON(int)	link_triple(rdf_db *db, triple *t, query *q);
COMMON(int)	postlink_triple(rdf_db *db, triple *t, query *q);
COMMON(void)	erase_triple(rdf_db *db, triple *t, query *q);
COMMON(void)	add_triple_consequences(rdf_db *db, triple *t, query *q);
COMMON(void)	del_triple_consequences(rdf_db *db, triple *t, query *q);
COMMON(predicate *) lookup_predicate(rdf_db *db, atom_t name);
COMMON(rdf_db*)	rdf_current_db(void);
COMMON(int)	rdf_broadcast(broadcast_id id, void *a1, void *a2);
COMMON(int)	rdf_is_broadcasting(broadcast_id id);
COMMON(void)	consider_triple_rehash(rdf_db *db, size_t extra);
COMMON(int)	rdf_create_gc_thread(rdf_db *db);
COMMON(atom_t)  expand_prefix(rdf_db *db, atom_t alias, atom_t local);

#include "error.h"

#endif /*RDFDB_H_INCLUDED*/
