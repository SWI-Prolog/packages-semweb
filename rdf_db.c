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

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define WITH_PL_MUTEX 1

#ifdef __WINDOWS__
#include <malloc.h>
#define inline __inline
#ifndef SIZEOF_LONG
#define SIZEOF_LONG 4
#endif
#endif

#include "rdf_db.h"
#include <wctype.h>
#include <ctype.h>
#include "murmur.h"
#include "memory.h"
#include "buffer.h"
#ifdef WITH_MD5
#include "md5.h"

#undef ERROR				/* also in wingdi.h; we do not care */
#define ERROR -1

static void md5_triple(triple *t, md5_byte_t *digest);
static void sum_digest(md5_byte_t *digest, md5_byte_t *add);
static void dec_digest(md5_byte_t *digest, md5_byte_t *add);
static int  md5_unify_digest(term_t t, md5_byte_t digest[16]);
#endif

void *
rdf_malloc(rdf_db *db, size_t size)
{ return malloc(size);
}

void
rdf_free(rdf_db *db, void *ptr, size_t size)
{ free(ptr);
}

static functor_t FUNCTOR_literal1;
static functor_t FUNCTOR_literal2;
static functor_t FUNCTOR_colon2;
static functor_t FUNCTOR_plus2;

static functor_t FUNCTOR_triples1;
static functor_t FUNCTOR_triples2;
static functor_t FUNCTOR_resources1;
static functor_t FUNCTOR_predicates1;
static functor_t FUNCTOR_duplicates1;
static functor_t FUNCTOR_lingering1;
static functor_t FUNCTOR_literals1;
static functor_t FUNCTOR_subject1;
static functor_t FUNCTOR_predicate1;
static functor_t FUNCTOR_object1;
static functor_t FUNCTOR_graph1;
static functor_t FUNCTOR_indexed16;
static functor_t FUNCTOR_hash_quality1;
static functor_t FUNCTOR_hash3;
static functor_t FUNCTOR_hash4;

static functor_t FUNCTOR_exact1;
static functor_t FUNCTOR_icase1;
static functor_t FUNCTOR_plain1;
static functor_t FUNCTOR_substring1;
static functor_t FUNCTOR_word1;
static functor_t FUNCTOR_prefix1;
static functor_t FUNCTOR_like1;
static functor_t FUNCTOR_lt1;
static functor_t FUNCTOR_le1;
static functor_t FUNCTOR_eq1;
static functor_t FUNCTOR_between2;
static functor_t FUNCTOR_ge1;
static functor_t FUNCTOR_gt1;

static functor_t FUNCTOR_symmetric1;
static functor_t FUNCTOR_inverse_of1;
static functor_t FUNCTOR_transitive1;
static functor_t FUNCTOR_rdf_subject_branch_factor1;    /* S --> BF*O */
static functor_t FUNCTOR_rdf_object_branch_factor1;	/* O --> BF*S */
static functor_t FUNCTOR_rdfs_subject_branch_factor1;	/* S --> BF*O */
static functor_t FUNCTOR_rdfs_object_branch_factor1;	/* O --> BF*S */

static functor_t FUNCTOR_searched_nodes1;
static functor_t FUNCTOR_lang2;
static functor_t FUNCTOR_type2;

static functor_t FUNCTOR_gc4;
static functor_t FUNCTOR_graphs1;

static functor_t FUNCTOR_assert4;
static functor_t FUNCTOR_retract4;
static functor_t FUNCTOR_update5;
static functor_t FUNCTOR_new_literal1;
static functor_t FUNCTOR_old_literal1;
static functor_t FUNCTOR_transaction2;
static functor_t FUNCTOR_load2;
static functor_t FUNCTOR_begin1;
static functor_t FUNCTOR_end1;
static functor_t FUNCTOR_create_graph1;

static atom_t   ATOM_user;
static atom_t	ATOM_exact;
static atom_t	ATOM_icase;
static atom_t	ATOM_plain;
static atom_t	ATOM_prefix;
static atom_t	ATOM_substring;
static atom_t	ATOM_word;
static atom_t	ATOM_like;
static atom_t	ATOM_error;
static atom_t	ATOM_begin;
static atom_t	ATOM_end;
static atom_t	ATOM_error;
static atom_t	ATOM_infinite;
static atom_t	ATOM_snapshot;
static atom_t	ATOM_true;
static atom_t	ATOM_size;
static atom_t	ATOM_optimize_threshold;
static atom_t	ATOM_average_chain_len;
static atom_t	ATOM_reset;
static atom_t	ATOM_lt;		/* < */
static atom_t	ATOM_eq;		/* = */
static atom_t	ATOM_gt;		/* > */

static atom_t	ATOM_subPropertyOf;
static atom_t	ATOM_xsdString;
static atom_t	ATOM_xsdDouble;

static predicate_t PRED_call1;

#define MATCH_EXACT		0x01	/* exact triple match */
#define MATCH_SUBPROPERTY	0x02	/* Use subPropertyOf relations */
#define MATCH_SRC		0x04	/* Match graph location */
#define MATCH_INVERSE		0x08	/* use symmetric match too */
#define MATCH_QUAL		0x10	/* Match qualifiers too */
#define MATCH_NUMERIC		0x20	/* Match typed objects numerically */
#define MATCH_DUPLICATE		(MATCH_EXACT|MATCH_QUAL)

static int match_triples(rdf_db *db, triple *t, triple *p,
			 query *q, unsigned flags);
static void unlock_atoms(rdf_db *db, triple *t);
static void lock_atoms(rdf_db *db, triple *t);
static void unlock_atoms_literal(literal *lit);

static size_t	triple_hash_key(triple *t, int which);
static size_t	object_hash(triple *t);
static void	mark_duplicate(rdf_db *db, triple *t, query *q);
static void	link_triple_hash(rdf_db *db, triple *t);
static void	free_triple(rdf_db *db, triple *t, int linger);

static sub_p_matrix *create_reachability_matrix(rdf_db *db,
						predicate_cloud *cloud,
						query *q);
static void	free_reachability_matrix(rdf_db *db, sub_p_matrix *rm);
static void	gc_is_leaf(rdf_db *db, predicate *p, gen_t gen);
static int	get_predicate(rdf_db *db, term_t t, predicate **p, query *q);
static int	get_existing_predicate(rdf_db *db, term_t t, predicate **p);
static void	free_bitmatrix(rdf_db *db, bitmatrix *bm);
static predicate_cloud *new_predicate_cloud(rdf_db *db,
					    predicate **p, size_t count);
static int	unify_literal(term_t lit, literal *l);
static int	free_literal(rdf_db *db, literal *lit);
static int	check_predicate_cloud(predicate_cloud *c);
static void	invalidate_is_leaf(predicate *p, query *q, int add);
static void	create_triple_hashes(rdf_db *db, int count, int *ic);
static void	free_literal_value(rdf_db *db, literal *lit);
static void	finalize_graph(void *g, void *db);


		 /*******************************
		 *	       LOCKING		*
		 *******************************/

static void
INIT_LOCK(rdf_db *db)
{ simpleMutexInit(&db->locks.literal);
  simpleMutexInit(&db->locks.misc);
  simpleMutexInit(&db->locks.gc);
  simpleMutexInit(&db->locks.duplicates);
  simpleMutexInit(&db->locks.erase);
  simpleMutexInit(&db->locks.prefixes);
}

static simpleMutex rdf_lock;


		 /*******************************
		 *	   DEBUG SUPPORT	*
		 *******************************/

#ifdef O_DEBUG

#define PRT_SRC	0x1				/* print source */
#define PRT_NL	0x2				/* add newline */
#define PRT_GEN	0x4				/* print generation info */
#define PRT_ADR	0x8				/* print address */

static void
print_literal(literal *lit)
{ switch(lit->objtype)
  { case OBJ_STRING:
      switch(lit->qualifier)
      { case Q_TYPE:
	  Sdprintf("%s^^\"%s\"",
		   PL_atom_chars(lit->value.string),
		   PL_atom_chars(ID_ATOM(lit->type_or_lang)));
	  break;
	case Q_LANG:
	  Sdprintf("%s@\"%s\"",
		   PL_atom_chars(lit->value.string),
		   PL_atom_chars(ID_ATOM(lit->type_or_lang)));
	  break;
	default:
	{ size_t len;
	  const char *s;
	  const wchar_t *w;

	  if ( (s = PL_atom_nchars(lit->value.string, &len)) )
	  { if ( strlen(s) == len )
	      Sdprintf("\"%s\"", s);
	    else
	      Sdprintf("\"%s\" (len=%d)", s, len);
	  } else if ( (w = PL_atom_wchars(lit->value.string, &len)) )
	  { unsigned int i;
	    Sputc('L', Serror);
	    Sputc('"', Serror);
	    for(i=0; i<len; i++)
	    { if ( w[i] < 0x7f )
		Sputc(w[i], Serror);
	      else
		Sfprintf(Serror, "\\\\u%04x", w[i]);
	    }
	    Sputc('"', Serror);
	  }
	  break;
	}
      }
      break;
    case OBJ_INTEGER:
      Sdprintf("%ld", lit->value.integer);
      break;
    case OBJ_DOUBLE:
      Sdprintf("%f", lit->value.real);
      break;
    case OBJ_TERM:
    { fid_t fid = PL_open_foreign_frame();
      term_t term = PL_new_term_ref();

      PL_recorded_external(lit->value.term.record, term);
      PL_write_term(Serror, term, 1200,
		    PL_WRT_QUOTED|PL_WRT_NUMBERVARS|PL_WRT_PORTRAY);
      PL_discard_foreign_frame(fid);
      break;
    }
    default:
      assert(0);
  }
}


static void
print_object(triple *t)
{ if ( t->object_is_literal )
  { print_literal(t->object.literal);
  } else
  { Sdprintf("%s", t->object.resource ? PL_atom_chars(t->object.resource) : "?o");
  }
}


static void
print_src(triple *t)
{ if ( t->graph_id )
  { if ( t->line == NO_LINE )
      Sdprintf(" [%s]", PL_atom_chars(ID_ATOM(t->graph_id)));
    else
      Sdprintf(" [%s:%ld]", PL_atom_chars(ID_ATOM(t->graph_id)), t->line);
  } else
  { Sdprintf(" ?g");
  }
}


static char *
triple_status_flags(triple *t, char *buf)
{ char *o = buf;

  *o++ = ' ';
  if ( t->atoms_locked )
    *o++ = 'L';
  if ( t->is_duplicate )
    *o++ = 'D';

  if ( o > buf+1 )
    *o = '\0';
  else
    buf[0] = '\0';

  return buf;
}


static void
print_gen(triple *t)
{ char buf[3][24];

  Sdprintf(" (%s..%s%s)",
	   gen_name(t->lifespan.born, buf[0]),
	   gen_name(t->lifespan.died, buf[1]),
	   triple_status_flags(t, buf[2]));
}


static void
print_triple(triple *t, int flags)
{ Sdprintf("<%s %s ",
	   t->subject_id ? PL_atom_chars(ID_ATOM(t->subject_id)) : "?s",
	   t->predicate.r->name ? PL_atom_chars(t->predicate.r->name) : "?p");
  print_object(t);
  if ( (flags & PRT_SRC) )
    print_src(t);
  if ( (flags & PRT_GEN) )
    print_gen(t);
  if ( (flags & PRT_ADR) )
    Sdprintf(" &%p", t);
  Sdprintf((flags & PRT_NL) ? ">\n" : ">");
}

#endif

		 /*******************************
		 *	     STORAGE		*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Tables that allow finding the hash-chains   for a particular index. They
are currently crafted by hand, such that the compiler knowns the mapping
is  constant.  check_index_tables()  verifies  that    the   tables  are
consistent.  To add an index:

    * Increment INDEX_TABLES in rdf_db.h
    * Add the index to col_index[]
    * Assign it a (consistent) position in index_col[]
    * If decide wich unindexed queries are best mapped
      to the new index and add them to alt_index[]
    * Add entries to col_name[], col_avg_len[], col_opt_threshold[]
    * Deal with the new index in consider_triple_rehash() and
      initial_size_triple_hash()

Make sure you compile with support for   assert(). If you make a mistake
in the above, you are likely  to   get  an  assertion failure. Thanks to
Haitao Zhang for debugging these notes.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#define ICOL(i) (index_col[i])

static const int index_col[16] =
{ 0,					/* BY_NONE */
  1,					/* BY_S */
  2,					/* BY_P */
  3,					/* BY_SP */
  4,					/* BY_O */
  ~0,					/* BY_SO */
  5,					/* BY_PO */
  6,					/* BY_SPO */

  7,					/* BY_G */
  8,					/* BY_SG */
  9,					/* BY_PG */
 ~0,					/* BY_SPG */
 ~0,					/* BY_OG */
 ~0,					/* BY_SOG */
 ~0,					/* BY_POG */
 ~0					/* BY_SPOG */
};

static int col_index[INDEX_TABLES] =
{ BY_NONE,
  BY_S,
  BY_P,
  BY_SP,
  BY_O,
  BY_PO,
  BY_SPO,
  BY_G,
  BY_SG,
  BY_PG
};

static const char *col_name[INDEX_TABLES] =
{ "-",
  "s",
  "p",
  "sp",
  "o",
  "po",
  "spo",
  "g",
  "sg",
  "pg"
};

static const int col_avg_len[INDEX_TABLES] =
{ 0,	/*BY_NONE*/
  2,	/*BY_S*/
  2,	/*BY_P*/
  2,	/*BY_SP*/
  4,	/*BY_O*/
  2,	/*BY_PO*/
  2,	/*BY_SPO*/
  1,	/*BY_G*/
  2,	/*BY_SG*/
  2	/*BY_PG*/
};

static const int col_opt_threshold[INDEX_TABLES] =
{ 0,	/*BY_NONE*/
  2,	/*BY_S*/
  2,	/*BY_P*/
  2,	/*BY_SP*/
  2,	/*BY_O*/
  2,	/*BY_PO*/
  2,	/*BY_SPO*/
  2,	/*BY_G*/
  2,	/*BY_SG*/
  2	/*BY_PG*/
};

static const int alt_index[16] =
{ BY_NONE,				/* BY_NONE */
  BY_S,					/* BY_S */
  BY_P,					/* BY_P */
  BY_SP,				/* BY_SP */
  BY_O,					/* BY_O */
  BY_S,					/* BY_SO */
  BY_PO,				/* BY_PO */
  BY_SPO,				/* BY_SPO */

  BY_G,					/* BY_G */
  BY_SG,				/* BY_SG */
  BY_PG,				/* BY_PG */
  BY_SP,				/* BY_SPG */
  BY_O,					/* BY_OG */
  BY_S,					/* BY_SOG */
  BY_PO,				/* BY_POG */
  BY_SPO				/* BY_SPOG */
};


static void
check_index_tables()
{ int i, ic;

  for(i=0; i<16; i++)
  { if ( (ic=index_col[i]) != ~0 )
    { assert(col_index[ic] == i);
    }
  }

  for(i=0; i<16; i++)
  { int ai = alt_index[i];

    assert(index_col[ai] != ~0);
  }

  for(i=0; i<INDEX_TABLES; i++)
  { ic = col_index[i];
    assert(alt_index[ic] == ic);
  }
}


		 /*******************************
		 *	      LISTS		*
		 *******************************/

static int
add_list(rdf_db *db, list *list, void *value)
{ cell *c;

  for(c=list->head; c; c=c->next)
  { if ( c->value == value )
      return FALSE;			/* already a member */
  }

  c = rdf_malloc(db, sizeof(*c));
  c->value = value;
  c->next = NULL;

  if ( list->tail )
    list->tail->next = c;
  else
    list->head = c;

  list->tail = c;

  return TRUE;
}


static int
del_list(rdf_db *db, list *list, void *value)
{ cell *c, *p = NULL;

  for(c=list->head; c; p=c, c=c->next)
  { if ( c->value == value )
    { if ( p )
	p->next = c->next;
      else
	list->head = c->next;

      if ( !c->next )
	list->tail = p;

      rdf_free(db, c, sizeof(*c));

      return TRUE;
    }
  }

  return FALSE;				/* not a member */
}


static void
free_list(rdf_db *db, list *list)
{ cell *c, *n;

  for(c=list->head; c; c=n)
  { n = c->next;
    rdf_free(db, c, sizeof(*c));
  }

  list->head = list->tail = NULL;
}


		 /*******************************
		 *	      TMP STORE		*
		 *******************************/

static void
init_tmp_store(tmp_store *s)
{ s->chunks = &s->store0;
  s->chunks->next = NULL;
  s->chunks->used = 0;
}


static void *
alloc_tmp_store(tmp_store *s, size_t size)
{ void *p;

  assert(size < CHUNKSIZE);

  if ( s->chunks->used + size > CHUNKSIZE )
  { mchunk *ch = malloc(sizeof(mchunk));

    ch->used = 0;
    ch->next = s->chunks;
    s->chunks = ch;
  }

  p = &s->chunks->buf[s->chunks->used];
  s->chunks->used += size;

  return p;
}


static void
destroy_tmp_store(tmp_store *s)
{ mchunk *ch, *next;

  for(ch=s->chunks; ch != &s->store0; ch = next)
  { next = ch->next;
    free(ch);
  }
}


		 /*******************************
		 *	     ATOM SETS		*
		 *******************************/

#define ATOMSET_INITIAL_ENTRIES 16

typedef struct atom_cell
{ struct atom_cell *next;
  atom_t     atom;
} atom_cell;

typedef struct
{ atom_cell **entries;			/* Hash entries */
  size_t      size;			/* Hash-table size */
  size_t      count;			/* # atoms stored */
  tmp_store   store;			/* Temporary storage */
  atom_cell  *entries0[ATOMSET_INITIAL_ENTRIES];
} atomset;


static void *
alloc_atomset(atomset *as, size_t size)
{ return alloc_tmp_store(&as->store, size);
}


static void
init_atomset(atomset *as)
{ init_tmp_store(&as->store);
  memset(as->entries0, 0, sizeof(as->entries0));
  as->entries = as->entries0;
  as->size = ATOMSET_INITIAL_ENTRIES;
  as->count = 0;
}


static void
destroy_atomset(atomset *as)
{ destroy_tmp_store(&as->store);

  if ( as->entries != as->entries0 )
    free(as->entries);
}


static void
rehash_atom_set(atomset *as)
{ size_t newsize = as->size*2;
  atom_cell **new = malloc(newsize*sizeof(atom_cell*));
  int i;

  memset(new, 0, newsize*sizeof(atom_cell*));

  for(i=0; i<as->size; i++)
  { atom_cell *c, *n;

    for(c=as->entries[i]; c; c=n)
    { size_t inew = atom_hash(c->atom, MURMUR_SEED)&(newsize-1);

      n = c->next;
      c->next = new[inew];
      new[inew] = c;
    }
  }

  if ( as->entries == as->entries0 )
  { as->entries = new;
  } else
  { atom_cell **old = as->entries;
    as->entries = new;
    free(old);
  }

  as->size = newsize;
}


static int
add_atomset(atomset *as, atom_t atom)
{ size_t i = atom_hash(atom, MURMUR_SEED)&(as->size-1);
  atom_cell *c;

  for(c=as->entries[i]; c; c=c->next)
  { if ( c->atom == atom )
      return 0;
  }

  if ( ++as->count > 2*as->size )
  { rehash_atom_set(as);
    i = atom_hash(atom, MURMUR_SEED)&(as->size-1);
  }

  c = alloc_atomset(as, sizeof(*c));
  c->atom = atom;
  c->next = as->entries[i];
  as->entries[i] = c;

  return 1;
}


static int
for_atomset(atomset *as,
	    int (*func)(atom_t a, void *closure),
	    void *closure)
{ int key;

  for(key=0; key < as->size; key++)
  { atom_cell *c;

    for(c=as->entries[key]; c; c=c->next)
    { if ( !(*func)(c->atom, closure) )
	return FALSE;
    }
  }

  return TRUE;
}


		 /*******************************
		 *	   TRIPLE SETS		*
		 *******************************/

/* Note that only ->entries need to be NULL to consider the set empty.
   The remainder of the initialization is done lazily.
*/

static void *
alloc_tripleset(void *ptr, size_t size)
{ tripleset *ts = ptr;

  return alloc_tmp_store(&ts->store, size);
}


static void
init_tripleset(tripleset *ts)
{ init_tmp_store(&ts->store);
  memset(ts->entries0, 0, sizeof(ts->entries0));
  ts->entries = ts->entries0;
  ts->size = TRIPLESET_INITIAL_ENTRIES;
  ts->count = 0;
}


static void
destroy_tripleset(tripleset *ts)
{ if ( ts->entries )
  { destroy_tmp_store(&ts->store);

    if ( ts->entries != ts->entries0 )
      free(ts->entries);
  }
}


static void
rehash_triple_set(tripleset *ts)
{ size_t newsize = ts->size*2;
  triple_cell **new = malloc(newsize*sizeof(triple_cell*));
  int i;

  memset(new, 0, newsize*sizeof(triple_cell*));

  for(i=0; i<ts->size; i++)
  { triple_cell *c, *n;

    for(c=ts->entries[i]; c; c=n)
    { size_t inew = triple_hash_key(c->triple, BY_SPO)&(newsize-1);

      n = c->next;
      c->next = new[inew];
      new[inew] = c;
    }
  }

  if ( ts->entries == ts->entries0 )
  { ts->entries = new;
  } else
  { triple_cell **old = ts->entries;
    ts->entries = new;
    free(old);
  }

  ts->size = newsize;
}


static int
add_tripleset(search_state *state, tripleset *ts, triple *triple)
{ size_t i;
  triple_cell *c;

  if ( !ts->entries )
    init_tripleset(ts);

  i = triple_hash_key(triple, BY_SPO)&(ts->size-1);
  for(c=ts->entries[i]; c; c=c->next)
  { if ( match_triples(state->db,
		       triple, c->triple,
		       state->query, MATCH_DUPLICATE) )
      return 0;
  }

  if ( ++ts->count > 2*ts->size )
  { rehash_triple_set(ts);
    i = triple_hash_key(triple, BY_SPO)&(ts->size-1);
  }

  c = alloc_tripleset(ts, sizeof(*c));
  c->triple = triple;
  c->next = ts->entries[i];
  ts->entries[i] = c;

  return 1;
}


		 /*******************************
		 *	      PREFIXES		*
		 *******************************/

static prefix_table *
new_prefix_table(void)
{ prefix_table *t = malloc(sizeof(*t));

  if ( t )
  { memset(t, 0, sizeof(*t));
    t->size    = PREFIX_INITIAL_ENTRIES;
    t->entries = malloc(t->size*sizeof(*t->entries));
    if ( t->entries )
    { memset(t->entries, 0, t->size*sizeof(*t->entries));
    } else
    { free(t);
      t = NULL;
    }
  }

  return t;
}


static void
empty_prefix_table(rdf_db *db)
{ int i;
  prefix_table *t = db->prefixes;

  simpleMutexLock(&db->locks.prefixes);
  for(i=0; i<t->size; i++)
  { prefix *p, *next;

    p = t->entries[i];
    t->entries[i] = NULL;
    for(; p; p = next)
    { next = p->next;

      PL_unregister_atom(p->alias);
      PL_unregister_atom(p->uri.handle);
      free(p);
    }
  }
  simpleMutexUnlock(&db->locks.prefixes);
  t->count = 0;

  flush_prefix_cache();
}


static void
resize_prefix_table(prefix_table *t)
{ size_t new_size = t->size*2;
  prefix **new_entries = malloc(new_size*sizeof(*new_entries));

  if ( new_entries )
  { int i;

    memset(new_entries, 0, new_size*sizeof(*new_entries));
    for(i=0; i<t->size; i++)
    { prefix *p, *next;

      for(p=t->entries[i]; p; p = next)
      { unsigned key = atom_hash(p->alias, MURMUR_SEED) & (new_size-1);

	next = p->next;
	p->next = new_entries[key];
	new_entries[key] = p;
      }
    }

    t->size = new_size;
    free(t->entries);
    t->entries = new_entries;
  }
}



static prefix *
add_prefix(rdf_db *db, atom_t alias, atom_t uri)
{ prefix_table *t = db->prefixes;
  unsigned key = atom_hash(alias, MURMUR_SEED) & (t->size-1);
  prefix *p = malloc(sizeof(*p));

  if ( !p )
  { PL_resource_error("memory");
    return NULL;
  }

  if ( t->count > t->size )
    resize_prefix_table(t);

  memset(p, 0, sizeof(*p));
  p->alias      = alias;
  p->uri.handle = uri;
  PL_register_atom(alias);
  PL_register_atom(uri);
  fill_atom_info(&p->uri);

  p->next = t->entries[key];
  t->entries[key] = p;
  t->count++;

  return p;
}


static prefix *
lookup_prefix(rdf_db *db, atom_t a)
{ prefix_table *t;
  prefix *pl;
  fid_t fid;
  static predicate_t pred = NULL;

  simpleMutexLock(&db->locks.prefixes);
  t = db->prefixes;
  for(pl = t->entries[atom_hash(a, MURMUR_SEED)&(t->size-1)]; pl; pl=pl->next)
  { if ( pl->alias == a )
    { simpleMutexUnlock(&db->locks.prefixes);
      return pl;
    }
  }

  if ( !pred )
    pred = PL_predicate("rdf_current_prefix", 2, "rdf_db");

  assert(pl == NULL);
  if ( (fid = PL_open_foreign_frame()) )
  { term_t av = PL_new_term_refs(2);
    atom_t uri_atom;

    PL_put_atom(av+0, a);
    if ( PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, pred, av) &&
	 PL_get_atom_ex(av+1, &uri_atom) )
      pl = add_prefix(db, a, uri_atom);
    else if ( !PL_exception(0) )
      PL_existence_error("rdf_prefix", av+0);

    PL_close_foreign_frame(fid);
  }

  simpleMutexUnlock(&db->locks.prefixes);

  return pl;
}


static wchar_t *
add_text(wchar_t *w, const text *t)
{ if ( t->a )
  { const unsigned char *a = t->a;
    const unsigned char *e = &a[t->length];

    for(; a<e; a++)
      *w++ = *a;
  } else
  { const wchar_t *a = t->w;
    const wchar_t *e = &a[t->length];

    for(; a<e; a++)
      *w++ = *a;
  }

  return w;
}


atom_t
expand_prefix(rdf_db *db, atom_t alias, atom_t local)
{ prefix *p = lookup_prefix(db, alias);

  if ( p )
  { atom_info ai = {0};
    ai.handle = local;
    fill_atom_info(&ai);
    atom_t uri;

    if ( ai.text.a && p->uri.text.a )
    { char buf[256];
      size_t len = ai.text.length + p->uri.text.length;
      char *a = len <= sizeof(buf) ? buf : malloc(len);

      if ( !len )
	return (atom_t)0;
      memcpy(a, p->uri.text.a, p->uri.text.length);
      memcpy(&a[p->uri.text.length], ai.text.a, ai.text.length);

      uri = PL_new_atom_nchars(len, a);
      if ( a != buf )
	free(a);
    } else
    { wchar_t buf[256];
      size_t len = ai.text.length + p->uri.text.length;
      wchar_t *w = len <= sizeof(buf)/sizeof(wchar_t)
				   ? buf
				   : malloc(len*sizeof(wchar_t));

      if ( !len )
	return (atom_t)0;
      w = add_text(w, &p->uri.text);
      w = add_text(w, &ai.text);

      uri = PL_new_atom_wchars(len, w);
      if ( w != buf )
	free(w);
    }

    return uri;
  }

  return (atom_t)0;
}



#ifdef COMPACT

		 /*******************************
		 *	   TRIPLE ARRAY		*
		 *******************************/

static triple_element *
alloc_array_slice(size_t count, triple_element **last)
{ size_t bytes = count*sizeof(triple_element);
  triple_element *slice = malloc(bytes);

  if ( slice )
  { triple_element *end = slice+count-1;
    triple_element *e, *n;

    for(e=slice; e<end; e=n)
    { n = e+1;
      e->fnext = n;
    }
    e->fnext = NULL;

    if ( last )
      *last = e;
  }

  return slice;
}

static void
free_array_slice(triple_array *a, triple_element *list, triple_element *last)
{ triple_element *o;

  do
  { o = a->freelist;
    last->fnext = o;
  } while ( !__sync_bool_compare_and_swap(&a->freelist, o, list) );
}

static int
init_triple_array(rdf_db *db)
{ triple_array *a = &db->triple_array;
  triple_element *slice = alloc_array_slice(TRIPLE_ARRAY_PREINIT, NULL);
  int i;

  for(i=0; i<MSB(TRIPLE_ARRAY_PREINIT); i++)
    a->blocks[i] = slice;

  a->freelist = slice->fnext;		/* simply ignore the first for id>0 */
  a->preinit  = TRIPLE_ARRAY_PREINIT;
  a->size     = TRIPLE_ARRAY_PREINIT;

  return TRUE;
}

static void
destroy_triple_array(rdf_db *db)
{ triple_array *a = &db->triple_array;
  int i;

  free(a->blocks[0]);
  for(i=MSB(a->preinit); i<MSB(a->size); i++)
  { triple_element *e = a->blocks[i];

    e += 1<<(i-1);
    free(e);
  }
  memset(a, 0, sizeof(*a));
}

static void
reset_triple_array(rdf_db *db)
{ destroy_triple_array(db);
  init_triple_array(db);
}

static void
resize_triple_array(rdf_db *db)
{ triple_array *a = &db->triple_array;
  int i = MSB(a->size);
  triple_element *last;
  triple_element *slice = alloc_array_slice(a->size, &last);

  if ( slice )
  { a->blocks[i] = slice - a->size;
    a->size *= 2;
    free_array_slice(a, slice, last);
  }
}

static triple_element *
fetch_triple_element(rdf_db *db, triple_id id)
{ return &db->triple_array.blocks[MSB(id)][id];
}

/* assign a new triple a place in the triple array
*/

static triple_id
register_triple(rdf_db *db, triple *t)
{ triple_array *a = &db->triple_array;
  triple_element *e;
  size_t slice_size;
  int i;

  do
  { if ( !(e=a->freelist) )
    { simpleMutexLock(&db->locks.misc);
      while ( !(e=a->freelist) )
	resize_triple_array(db);
      simpleMutexUnlock(&db->locks.misc);
    }
  } while ( !__sync_bool_compare_and_swap(&a->freelist, e, e->fnext) );

  e->triple = t;

  for(i=1,slice_size=1; i<MAX_TBLOCKS; i++,slice_size*=2)
  { if ( e >= a->blocks[i]+slice_size &&
	 e <  a->blocks[i]+slice_size*2 )
    { t->id = e - a->blocks[i];

      assert(fetch_triple(db, t->id) == t);
      return t->id;
    }
  }

  assert(0);
  return 0;
}

static void
unregister_triple(rdf_db *db, triple *t)
{ if ( t->id != TRIPLE_NO_ID )
  { triple_element *e = fetch_triple_element(db, t->id);

    t->id = TRIPLE_NO_ID;
    free_array_slice(&db->triple_array, e, e);
  }
}

static triple *
triple_follow_hash(rdf_db *db, triple *t, int icol)
{ triple_id nid = t->tp.next[icol];

  return fetch_triple(db, nid);
}

#define T_ID(t) ((t) ? (t)->id : 0)

#else /*COMPACT*/

#define init_triple_array(db) (void)0
#define reset_triple_array(db) (void)0
#define register_triple(db, t) (void)0
#define unregister_triple(db, t) (void)0
#define triple_follow_hash(db, t, icol) ((t)->tp.next[icol])
#define T_ID(t) (t)

#endif /*COMPACT*/

static void
finalize_triple(void *data, void *client)
{ triple *t = data;
  rdf_db *db = client;

  if ( !db->resetting )
  { unlock_atoms(db, t);
    if ( t->object_is_literal && t->object.literal )
      free_literal(db, t->object.literal);
#ifdef COMPACT
      unregister_triple(db, t);
#endif
  }
  SECURE(memset(t, 0, sizeof(*t)));
  TMAGIC(t, T_FREED);
  ATOMIC_SUB(&db->lingering, 1);
}


		 /*******************************
		 *	  TRIPLE WALKER		*
		 *******************************/

/* init_triple_walker() and next_triple() are the primitives to walk indexed
   triples.  The pattern is:

	triple_walker tw;

	init_triple_walker(&tw, db, pattern, index);
	while((t=next_triple(tw)))
	  <do your job>

  TBD: Get the generation into this story.  Most likely it is better to
  deal with this in this low-level loop then outside. We will handle
  this in the next cycle.
*/

static void
init_triple_walker(triple_walker *tw, rdf_db *db, triple *pattern, int which)
{ tw->unbounded_hash = triple_hash_key(pattern, which);
  tw->current	     = NULL;
  tw->icol	     = ICOL(which);
  tw->db	     = db;
  if ( !tw->db->hash[tw->icol].created )
    create_triple_hashes(db, 1, &tw->icol);
  tw->bcount	     = tw->db->hash[tw->icol].bucket_count_epoch;
}


static void
init_triple_literal_walker(triple_walker *tw, rdf_db *db,
			   triple *pattern, int which, unsigned int hash)
{ tw->unbounded_hash = hash;
  tw->current	     = NULL;
  tw->icol	     = ICOL(which);
  tw->db	     = db;
  if ( !tw->db->hash[tw->icol].created )
    create_triple_hashes(db, 1, &tw->icol);
  tw->bcount	     = tw->db->hash[tw->icol].bucket_count_epoch;
}


static void
rewind_triple_walker(triple_walker *tw)
{ tw->bcount  = tw->db->hash[tw->icol].bucket_count_epoch;
  tw->current = NULL;
}


static triple *
next_hash_triple(triple_walker *tw)
{ triple *rc;
  triple_hash *hash = &tw->db->hash[tw->icol];

  if ( tw->bcount <= hash->bucket_count )
  { do
    { int entry = tw->unbounded_hash % tw->bcount;
      triple_bucket *bucket = &hash->blocks[MSB(entry)][entry];

      rc = fetch_triple(tw->db, bucket->head);
      do
      { tw->bcount *= 2;
      } while ( tw->bcount <= hash->bucket_count &&
		tw->unbounded_hash % tw->bcount == entry );
    } while(!rc && tw->bcount <= hash->bucket_count );

    if ( rc )
      tw->current = triple_follow_hash(tw->db, rc, tw->icol);
  } else
  { rc = NULL;
  }

  return rc;
}


static inline triple *
next_triple(triple_walker *tw)
{ triple *rc;

  if ( (rc=tw->current) )
  { tw->current = triple_follow_hash(tw->db, rc, tw->icol);

    return rc;
  } else
  { return next_hash_triple(tw);
  }
}


static inline void
destroy_triple_walker(rdf_db *db, triple_walker *tw)
{
}


		 /*******************************
		 *	    PREDICATES		*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Predicates are represented as first class   citizens  for three reasons:
quickly  answer  on  the  transitive   rdfs:subPropertyOf  relation  for
rdf_hash/3,  keep  track  of  statistics  that   are  useful  for  query
optimization  (#triples,  branching   factor)    and   keep   properties
(inverse/transitive).

To answer the rdfs:subPropertyOf quickly,   predicates  are organised in
`clouds', where a cloud defines a   set  of predicates connected through
rdfs:subPropertyOf triples. The cloud numbers  its members and maintains
a bit-matrix that contains the closure  of the reachability. Initially a
predicate has a simple cloud of size 1. merge_clouds() and split_cloud()
deals with adding  and  deleting   rdfs:subPropertyOf  relations.  These
operations try to modify the clouds that have   no triples, so it can be
done without a rehash. If this fails, the predicates keep their own hash
to make search without rdfs:subPropertyOf  still   possible  (so  we can
avoid frequent updates while loading triples),   sets  the cloud `dirty'
flag and the DB's need_update flag. Queries that need rdfs:subPropertyOf
find the need_update flag,  which   calls  organise_predicates(),  which
cause a rehash if some predicates  have   changed  hash-code  to the new
cloud they have become part of.

TBD: We can do a partial re-hash in that case!
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
init_pred_table(rdf_db *db)
{ size_t bytes = sizeof(predicate**)*INITIAL_PREDICATE_TABLE_SIZE;
  predicate **p = PL_malloc_uncollectable(bytes);
  int i, count = INITIAL_PREDICATE_TABLE_SIZE;

  memset(p, 0, bytes);
  for(i=0; i<MSB(count); i++)
    db->predicates.blocks[i] = p;

  db->predicates.bucket_count       = count;
  db->predicates.bucket_count_epoch = count;
  db->predicates.count              = 0;

  return TRUE;
}


static int
resize_pred_table(rdf_db *db)
{ int i = MSB(db->predicates.bucket_count);
  size_t bytes  = sizeof(predicate**)*db->predicates.bucket_count;
  predicate **p = PL_malloc_uncollectable(bytes);

  memset(p, 0, bytes);
  db->predicates.blocks[i] = p-db->predicates.bucket_count;
  db->predicates.bucket_count *= 2;
  DEBUG(1, Sdprintf("Resized predicate table to %ld\n",
		    (long)db->predicates.bucket_count));

  return TRUE;
}


typedef struct pred_walker
{ rdf_db       *db;			/* RDF DB */
  atom_t	name;			/* Name of the predicate */
  size_t	unbounded_hash;		/* Atom's hash */
  size_t	bcount;			/* current bucket count */
  predicate    *current;		/* current location */
} pred_walker;


static void
init_predicate_walker(pred_walker *pw, rdf_db *db, atom_t name)
{ pw->db	     = db;
  pw->name	     = name;
  pw->unbounded_hash = atom_hash(name, MURMUR_SEED);
  pw->bcount	     = db->predicates.bucket_count_epoch;
  pw->current	     = NULL;
}

static predicate*
next_predicate(pred_walker *pw)
{ predicate *p;

  if ( pw->current )
  { p = pw->current;
    pw->current = p->next;
  } else if ( pw->bcount <= pw->db->predicates.bucket_count )
  { do
    { int entry = pw->unbounded_hash % pw->bcount;
      p = pw->db->predicates.blocks[MSB(entry)][entry];
      pw->bcount *= 2;
    } while(!p && pw->bcount <= pw->db->predicates.bucket_count );

    if ( p )
      pw->current = p->next;
  } else
    return NULL;

  return p;
}


static predicate *
existing_predicate(rdf_db *db, atom_t name)
{ pred_walker pw;
  predicate *p;

  init_predicate_walker(&pw, db, name);
  while((p=next_predicate(&pw)))
  { if ( p->name == name )
      return p;
  }

  return NULL;
}


predicate *
lookup_predicate(rdf_db *db, atom_t name)
{ predicate *p, **pp;
  predicate_cloud *cp;
  int entry;

  if ( (p=existing_predicate(db, name)) )
    return p;

  LOCK_MISC(db);
  if ( (p=existing_predicate(db, name)) )
  { UNLOCK_MISC(db);
    return p;
  }

  p = rdf_malloc(db, sizeof(*p));
  memset(p, 0, sizeof(*p));
  p->name = name;
  cp = new_predicate_cloud(db, &p, 1);
  p->hash = cp->hash;
  PL_register_atom(name);
  if ( db->predicates.count > db->predicates.bucket_count )
    resize_pred_table(db);
  entry = atom_hash(name, MURMUR_SEED) % db->predicates.bucket_count;
  pp = &db->predicates.blocks[MSB(entry)][entry];
  p->next = *pp;
  *pp = p;
  db->predicates.count++;
  DEBUG(5, Sdprintf("Pred %s (count = %d)\n",
		    PL_atom_chars(name), db->predicates.count));
  UNLOCK_MISC(db);

  return p;
}


static const char *
pname(predicate *p)
{ if ( p->name )
    return PL_atom_chars(p->name);
  else
  { static char *ring[10];
    static int ri = 0;
    char buf[25];
    char *r;

    Ssprintf(buf, "__D%p", p);
    ring[ri++] = r = strdup(buf);
    if ( ri == 10 )
    { ri = 0;
      free(ring[ri]);
    }

    return (const char*)r;
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Keep track of the triple count.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static inline void
register_predicate(rdf_db *db, triple *t)
{ ATOMIC_ADD(&t->predicate.r->triple_count, 1);
}


static inline void
unregister_predicate(rdf_db *db, triple *t)
{ ATOMIC_SUB(&t->predicate.r->triple_count, 1);
}


		 /*******************************
		 *	 PREDICATE CLOUDS	*
		 *******************************/

static predicate_cloud *
new_predicate_cloud(rdf_db *db, predicate **p, size_t count)
{ predicate_cloud *cloud = rdf_malloc(db, sizeof(*cloud));

  memset(cloud, 0, sizeof(*cloud));
  cloud->hash = rdf_murmer_hash(&cloud, sizeof(cloud), PRED_MURMUR_SEED);
  if ( count )
  { int i;
    predicate **p2;

    cloud->size = count;
    cloud->members = rdf_malloc(db, sizeof(predicate*)*count);
    memcpy(cloud->members, p, sizeof(predicate*)*count);

    for(i=0, p2=cloud->members; i<cloud->size; i++, p2++)
    { (*p2)->cloud = cloud;
      (*p2)->label = i;
    }
  }

  return cloud;
}


static void
finalize_cloud(void *data, void *client)
{ rdf_db *db = client;
  predicate_cloud *cloud = data;
  sub_p_matrix *rm, *rm2;

  if ( cloud->members )
    rdf_free(db, cloud->members, sizeof(predicate*)*cloud->size);

  for(rm=cloud->reachable; rm; rm=rm2)
  { rm2 = rm->older;

    free_reachability_matrix(db, rm);
  }
}


static void
free_predicate_cloud(rdf_db *db, predicate_cloud *cloud)
{ finalize_cloud(cloud, db);

  rdf_free(db, cloud, sizeof(*cloud));
}


static size_t
triples_in_predicate_cloud(predicate_cloud *cloud)
{ size_t triples = 0;
  predicate **p;
  int i;

  for(i=0, p=cloud->members; i<cloud->size; i++, p++)
    triples += (*p)->triple_count;

  return triples;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
gc_cloud() removes old reachability matrices.   As  the query generation
has passed, we can immediately remove the  old bitmap. We must leave the
sub_p_matrix struct to GC as someone might be walking the chain.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void
gc_cloud(rdf_db *db, predicate_cloud *cloud, gen_t gen)
{ sub_p_matrix *rm, *older;
  sub_p_matrix *prev = NULL;

  for(rm=cloud->reachable; rm; rm=older)
  { older = rm->older;

    if ( rm->lifespan.died < gen )
    { if ( prev )
      { prev->older = older;
      } else
      { simpleMutexLock(&db->locks.misc);   /* sync with */
	cloud->reachable = older;	    /* create_reachability_matrix() */
	simpleMutexUnlock(&db->locks.misc);
      }

      free_bitmatrix(db, rm->matrix);
      rm->matrix = NULL;		    /* Clean to avoid false pointers */
      memset(&rm->lifespan, 0, sizeof(rm->lifespan));
      deferred_free(&db->defer_clouds, rm);
    } else
    { prev = rm;
    }
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
GC all clouds. We walk the predicates and   keep  a flag on the cloud in
which GC run it was collected to avoid collecting a cloud multiple times
in the same GC run. Alternatively,  we   could  keep  a list of possibly
dirty clouds, but that is more complicated and most likely not worth the
trouble. Afterall, we might walk  many   predicates  for few clouds, but
generally the number of predicates is still small compared to the number
of triples and thus the total cost in the GC process will be small.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
gc_clouds(rdf_db *db, gen_t gen)
{ int i;
  int gc_id = db->gc.count+1;

  enter_scan(&db->defer_all);
  for(i=0; i<db->predicates.bucket_count; i++)
  { predicate *p = db->predicates.blocks[MSB(i)][i];

    for( ; p; p = p->next )
    { if ( p->cloud->last_gc != gc_id )
      { p->cloud->last_gc = gc_id;

	gc_cloud(db, p->cloud, gen);
	if ( PL_handle_signals() < 0 )
	  return -1;
      }
      gc_is_leaf(db, p, gen);
    }
  }
  exit_scan(&db->defer_all);

  return 0;
}


static void
invalidateReachability(predicate_cloud *cloud, query *q)
{ sub_p_matrix *rm;
  gen_t gen_max = query_max_gen(q);

  for(rm=cloud->reachable; rm; rm=rm->older)
  { if ( rm->lifespan.died == gen_max )
      rm->lifespan.died = queryWriteGen(q);
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Append the predicates from cloud C2 to those of cloud C1.  There are two
scenarios:

  - C2 has no triples.  We are in a writer lock.  As there are no
    triples for C2, queries cannot go wrong.
  - C2 has triples.  It is possible that queries with the predicate
    hash of C2 are in progress.  See comment at merge_clouds() for
    how this is handled.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static predicate_cloud *
append_clouds(rdf_db *db,
	      predicate_cloud *c1, predicate_cloud *c2,
	      int update_hash)
{ int i;
  predicate **new_members;
  predicate **old_members = c1->members;

  new_members = rdf_malloc(db, (c1->size+c2->size)*sizeof(predicate*));
  memcpy(&new_members[0],        c1->members, c1->size*sizeof(predicate*));
  memcpy(&new_members[c1->size], c2->members, c2->size*sizeof(predicate*));
  c1->members = new_members;
  deferred_free(&db->defer_clouds, old_members);

					/* re-label the new ones */
  for(i=c1->size; i<c1->size+c2->size; i++)
  { predicate *p = c1->members[i];

    p->cloud = c1;
    p->label = i;
    if ( update_hash )
      p->hash = c1->hash;
  }
  c1->size += c2->size;

  if ( !update_hash )
  { size_t newc = 0;

    if ( c1->alt_hash_count )
      newc += c1->alt_hash_count;
    else
      newc++;

    if ( c2->alt_hash_count )
      newc += c2->alt_hash_count;
    else
      newc++;

    DEBUG(1, Sdprintf("Cloud %p: %d alt-hashes\n", c1, newc));

    if ( c1->alt_hashes )
    { unsigned int *new_hashes;
      unsigned int *old_hashes = c1->alt_hashes;

      new_hashes = rdf_malloc(db, newc*sizeof(unsigned int));
      memcpy(&new_hashes[0], c1->alt_hashes,
	     c1->alt_hash_count*sizeof(unsigned int));
      MEMORY_BARRIER();
      c1->alt_hashes = new_hashes;
      deferred_free(&db->defer_clouds, old_hashes);
    } else
    { c1->alt_hashes = rdf_malloc(db, newc*sizeof(unsigned int));
      c1->alt_hashes[0] = c1->hash;
      MEMORY_BARRIER();
      c1->alt_hash_count = 1;
    }

    if ( c2->alt_hash_count )
    { memcpy(&c1->alt_hashes[c1->alt_hash_count],
	     c2->alt_hashes, c2->alt_hash_count*sizeof(unsigned int));
    } else
    { c1->alt_hashes[c1->alt_hash_count] = c2->hash;
    }
    MEMORY_BARRIER();
    c1->alt_hash_count = newc;
  }

  deferred_finalize(&db->defer_clouds, c2,
		    finalize_cloud, db);

  return c1;
}


/* merge two predicate clouds. Note that this code is only called
   from addSubPropertyOf().  If c1==c2, we added an rdfs:subPropertyOf
   between two predicates in the same cloud. we must still invalidate
   the matrix.
*/

static predicate_cloud *
merge_clouds(rdf_db *db, predicate_cloud *c1, predicate_cloud *c2, query *q)
{ predicate_cloud *cloud;

  if ( c1 != c2 )
  { size_t tc1, tc2;

    if ( (tc1=triples_in_predicate_cloud(c1)) == 0 )
    { cloud = append_clouds(db, c2, c1, TRUE);
    } else if ( (tc2=triples_in_predicate_cloud(c2)) == 0 )
    { cloud = append_clouds(db, c1, c2, TRUE);
    } else
    { predicate_cloud *reindex;

      if ( tc2 < tc1 )
      { cloud = c1;
	reindex = c2;
      } else
      { cloud = c2;
	reindex = c1;
      }

      cloud = append_clouds(db, cloud, reindex, FALSE);
    }
  } else
  { cloud = c1;
  }

  invalidateReachability(cloud, q);

  return cloud;
}


static size_t
predicate_hash(predicate *p)
{ return p->hash;
}


static void
addSubPropertyOf(rdf_db *db, triple *t, query *q)
{ predicate *sub   = lookup_predicate(db, ID_ATOM(t->subject_id));
  predicate *super = lookup_predicate(db, t->object.resource);

  DEBUG(3, Sdprintf("addSubPropertyOf(%s, %s)\n",
		    pname(sub), pname(super)));

  invalidate_is_leaf(super, q, TRUE);

  if ( add_list(db, &sub->subPropertyOf, super) )
  { add_list(db, &super->siblings, sub);
    merge_clouds(db, sub->cloud, super->cloud, q);
  } else
  { predicate_cloud *cloud;

    cloud = super->cloud;
    assert(cloud == sub->cloud);

    invalidateReachability(cloud, q);
  }
}


/* deleting an rdfs:subPropertyOf.  This is a bit naughty.  If the
   cloud is still connected we only need to refresh the reachability
   matrix.  Otherwise the cloud breaks in maximum two clusters.  We
   can decide to leave it as is, which is simpler to implement
   but harms indexing.

   TBD: If the cloud becomes disconnected, it may be split.
*/

static void
delSubPropertyOf(rdf_db *db, triple *t, query *q)
{ predicate *sub   = lookup_predicate(db, ID_ATOM(t->subject_id));
  predicate *super = lookup_predicate(db, t->object.resource);
  predicate_cloud *cloud;

  DEBUG(3, Sdprintf("delSubPropertyOf(%s, %s)\n",
		    pname(sub), pname(super)));

  invalidate_is_leaf(super, q, FALSE);

  if ( del_list(db, &sub->subPropertyOf, super) )
  { del_list(db, &super->siblings, sub);
  }

  cloud = super->cloud;
  assert(cloud == sub->cloud);

  invalidateReachability(cloud, q);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Reachability matrix.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#define WBITSIZE (sizeof(int)*8)

static size_t
byte_size_bitmatrix(size_t w, size_t h)
{ size_t wsize = ((w*h)+WBITSIZE-1)/WBITSIZE;

  return (size_t)(intptr_t)&((bitmatrix*)NULL)->bits[wsize];
}


static bitmatrix *
alloc_bitmatrix(rdf_db *db, size_t w, size_t h)
{ size_t size = byte_size_bitmatrix(w, h);
  bitmatrix *m = rdf_malloc(db, size);

  memset(m, 0, size);
  m->width = w;
  m->heigth = h;

  return m;
}


static void
free_bitmatrix(rdf_db *db, bitmatrix *bm)
{ size_t size = byte_size_bitmatrix(bm->width, bm->heigth);

  rdf_free(db, bm, size);
}


#undef setbit				/* conflict in HPUX 11.23 */

static void
setbit(bitmatrix *m, int i, int j)
{ size_t ij = m->width*i+j;
  size_t word = ij/WBITSIZE;
  int bit  = ij%WBITSIZE;

  m->bits[word] |= 1<<bit;
}


static int
testbit(bitmatrix *m, int i, int j)
{ size_t ij = m->width*i+j;
  size_t word = ij/WBITSIZE;
  int bit  = ij%WBITSIZE;

  return ((m->bits[word] & (1<<bit)) != 0);
}


static int
check_labels_predicate_cloud(predicate_cloud *cloud)
{ predicate **p;
  int i;

  for(i=0, p=cloud->members; i<cloud->size; i++, p++)
    assert((*p)->label == i);

  return i;
}

static void
update_valid(lifespan *valid, gen_t change)
{ if ( change < valid->died )
  { if ( valid->died <= GEN_MAX ||	/* both non-transaction */
	 change > GEN_MAX )		/* both in transaction */
      valid->died = change;
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Match triple t against pattern p in query q. Update the died-property of
valid if the triple matches now,  but   will  not  after some generation
(i.e., it will die) or the triple must still be born.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static triple *
matching_object_triple_until(rdf_db *db, triple *t, triple *p, query *q,
			     unsigned flags, lifespan *valid)
{ triple *t2;

  if ( (t2=alive_triple(q, t)) )
  { if ( match_triples(db, t2, p, q, 0) &&
	 !t2->object_is_literal )	/* object properties only */
    { if ( t2->lifespan.died != query_max_gen(q) )
      { DEBUG(1, Sdprintf("Limit lifespan due to dead: ");
	      print_triple(t2, PRT_GEN|PRT_NL));
	update_valid(valid, t2->lifespan.died);
      }

      return t2;
    }
  } else
  { t2 = deref_triple(db, t);		/* Dubious */

    if ( match_triples(db, t2, p, q, 0) &&
	 !t2->object_is_literal )
    { if ( !t2->erased &&
	   !born_lifespan(q, &t2->lifespan) )
      { DEBUG(1, Sdprintf("Limit lifespan due to new born: ");
	      print_triple(t2, PRT_GEN|PRT_NL));
	update_valid(valid, t2->lifespan.born);
      }
    }
  }

  return NULL;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
fill_reachable()   computes   that    transitive     closure    of   the
rdfs:subPropertyOf relation. In addition, it   maintains  the generation
valid_until, which expresses  the  maximum   generation  until  when the
reachability  matrix  is  valid.  This  is    needed  if  we  compute  a
reachability matrix for an older generation.

TBD: The code below probably doesn't  work properly inside a transaction
due  to  the  complicated  generation  reasoning  there.  This  must  be
clarified and cleaned.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void
fill_reachable(rdf_db *db,
	       predicate_cloud *cloud,
	       bitmatrix *bm,
	       predicate *p0, predicate *p,
	       query *q,
	       lifespan *valid)
{ if ( !testbit(bm, p0->label, p->label) )
  { triple pattern;
    triple *t;
    triple_walker tw;

    memset(&pattern, 0, sizeof(pattern));

    DEBUG(3, Sdprintf("    Reachable [%s (%d)]\n", pname(p), p->label));
    setbit(bm, p0->label, p->label);
    pattern.subject_id = ATOM_ID(p->name);
    pattern.predicate.r = existing_predicate(db, ATOM_subPropertyOf);
    init_triple_walker(&tw, db, &pattern, BY_SP);
    while((t=next_triple(&tw)))
    { triple *t2;

      if ( (t2=matching_object_triple_until(db, t, &pattern, q, 0, valid)) )
      { predicate *super;

	super = lookup_predicate(db, t2->object.resource);
	assert(super->cloud == cloud);
	fill_reachable(db, cloud, bm, p0, super, q, valid);
      }
    }
  }
}


static int
is_transaction_start_gen(gen_t gen)
{ return (gen-GEN_TBASE)%GEN_TNEST == 0;
}


static void
init_valid_lifespan(rdf_db *db, lifespan *span, query *q)
{ if ( q->transaction && !is_transaction_start_gen(q->tr_gen) )
  { span->born = q->tr_gen;
    span->died = query_max_gen(q);
    add_list(db, &q->transaction->transaction_data.lifespans, span);
  } else
  { span->born = q->rd_gen;
    span->died = GEN_MAX;
  }
}



static sub_p_matrix *
create_reachability_matrix(rdf_db *db, predicate_cloud *cloud, query *q)
{ bitmatrix *m = alloc_bitmatrix(db, cloud->size, cloud->size);
  sub_p_matrix *rm = rdf_malloc(db, sizeof(*rm));
  predicate **p;
  int i;

  init_valid_lifespan(db, &rm->lifespan, q);

  DEBUG(1, { char buf[4][24];
	     Sdprintf("Create matrix for q at %s/%s, valid %s..%s\n",
		      gen_name(q->rd_gen, buf[0]),
		      gen_name(q->tr_gen, buf[1]),
		      gen_name(rm->lifespan.born, buf[2]),
		      gen_name(rm->lifespan.died, buf[3]));
	   });

  check_labels_predicate_cloud(cloud);
  for(i=0, p=cloud->members; i<cloud->size; i++, p++)
  { DEBUG(2, Sdprintf("Reachability for %s (%d)\n", pname(*p), (*p)->label));

    fill_reachable(db, cloud, m, *p, *p, q, &rm->lifespan);
  }

  DEBUG(1, { char buf[2][24];
	     Sdprintf("Created matrix, valid %s..%s\n",
		      gen_name(rm->lifespan.born, buf[0]),
		      gen_name(rm->lifespan.died, buf[1]));
	   });

  rm->matrix = m;
  simpleMutexLock(&db->locks.misc);		/* sync with gc_cloud() */
  rm->older = cloud->reachable;
  MEMORY_BARRIER();
  cloud->reachable = rm;
  simpleMutexUnlock(&db->locks.misc);

  return rm;
}


/* FIXME: we probably cannot guarantee these are not being
   accessed.  I.e., we must use GC lingering on them
*/

static void
free_reachability_matrix(rdf_db *db, sub_p_matrix *rm)
{ free_bitmatrix(db, rm->matrix);

  rdf_free(db, rm, sizeof(*rm));
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
isSubPropertyOf() is true if sub is an rdfs:subPropertyOf p (transitive)
for  the  given  query  q.  If  two  predicates  are  connected  through
rdfs:subPropertyOf, they belong to the same `cloud'. The cloud keeps one
or more bitmatrices  with  the   entailment  of  all  rdfs:subPropertyOf
triples. Each bitmatrix is  valid  during   a  certain  lifespan (set of
generations).

isSubPropertyOf() runs concurrently with updates and  must be careful in
its  processing  to   deal   with    the   modifications   realised   by
addSubPropertyOf() and delSubPropertyOf().  The  critical   path  is  if
addSubPropertyOf() connects two clouds, both  having multiple predicates
and both clouds have triples.

It is solved as follows. Suppose cloud C2   is  merged into cloud C1, we
take the following steps:

  - The predicates from C2 are added at the end of the ->members of C1.
    C1->size is updated.
    - This has no consequences for running queries that need the old
      entailment of the subPropertyOf anyway.
  - The cloud C2 gets ->merged_into set to C1
    - The cloud of a predicate is reached by following the ->merged_into
      chain. If such a link is followed, predicate->label (the index in
      the predicate cloud) is invalid and we must compute it.
  - For each member of C2
    - update <-label to the label in C1
      update <-cloud to C1
    - Leave C2 to Boehm-GC
  - Add the hash-key of C2 to the alt-hashes of C1.  Queries that
    involve sub-property on C1 must re-run the query with each
    alt-hash for that has a predicate that is a sub-property of
    the target.  TBD: find a good compromise between computing and
    storing yet additional closures.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static predicate_cloud *
cloud_of(predicate *p, int *labelp)
{ predicate_cloud *pc = p->cloud;
  int i;

  if ( !pc->merged_into )
  { *labelp = p->label;
    return pc;
  }

  while(!pc->merged_into)
    pc = pc->merged_into;

  for(i=0; i<pc->size; i++)
  { if ( pc->members[i] == p )
      *labelp = i;
    return pc;
  }

  assert(0);
  return 0;
}


static int
isSubPropertyOf(rdf_db *db, predicate *sub, predicate *p, query *q)
{ predicate_cloud *pc;
  int sub_label, p_label;

  assert(sub != p);

  pc = cloud_of(sub, &sub_label);
  if ( pc == cloud_of(p, &p_label) )
  { sub_p_matrix *rm;
    int max_label = (sub_label > p_label ? sub_label : p_label);

    for(rm=pc->reachable; rm; rm=rm->older)
    { if ( alive_lifespan(q, &rm->lifespan) &&
	   max_label < rm->matrix->width )
	return testbit(rm->matrix, sub_label, p_label);
    }

    if ( (rm = create_reachability_matrix(db, pc, q)) )
    { assert(alive_lifespan(q, &rm->lifespan));
      return testbit(rm->matrix, sub_label, p_label);
    } else
      assert(0);
  }

  return FALSE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
is_leaf_predicate()  is  true   if   p   has    no   children   in   the
rdfs:subPropertyOf tree at query q. We cache this information.

FIXME: Note that this code is subject to  race conditions. If we want to
avoid that without using locks, we must  put the validity information in
a seperate object that is not modified.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
is_leaf_predicate(rdf_db *db, predicate *p, query *q)
{ is_leaf *data;
  triple pattern;
  triple_walker tw;
  triple *t;

  memset(&pattern, 0, sizeof(pattern));

  for( data=p->is_leaf; data; data=data->older )
  { if ( alive_lifespan(q, &data->lifespan) )
      return data->is_leaf;
  }

  data = rdf_malloc(db, sizeof(*data));
  init_valid_lifespan(db, &data->lifespan, q);

  if ( (pattern.predicate.r = existing_predicate(db, ATOM_subPropertyOf)) )
  { pattern.object.resource = p->name;

    init_triple_walker(&tw, db, &pattern, BY_PO);
    while((t=next_triple(&tw)))
    { triple *t2;

      if ( (t2=matching_object_triple_until(db, t, &pattern, q, 0,
					    &data->lifespan)) )
      { data->is_leaf = FALSE;
	break;
      } else
	data->is_leaf = TRUE;
    }
  } else				/* rdfs:subPropertyOf doesn't exist */
  { data->is_leaf = TRUE;		/* so all preds are leafs  */
  }

  simpleMutexLock(&db->locks.misc);
  data->older = p->is_leaf;
  MEMORY_BARRIER();
  p->is_leaf = data;
  simpleMutexUnlock(&db->locks.misc);

  return data->is_leaf;
}


/* invalidate the is_leaf status if a sub-property is added/deleted.
   no need to do so if we add a child to a non-leaf.
*/

static void
invalidate_is_leaf(predicate *p, query *q, int add)
{ gen_t gen_max = query_max_gen(q);
  is_leaf *il;

  for(il=p->is_leaf; il; il=il->older)
  { if ( il->lifespan.died == gen_max )
    { if ( !(add && !il->is_leaf) )
	il->lifespan.died = queryWriteGen(q);
    }
  }
}


static void
gc_is_leaf(rdf_db *db, predicate *p, gen_t gen)
{ is_leaf *il, *older;
  is_leaf *prev = NULL;

  for(il = p->is_leaf; il; il=older)
  { older = il->older;

    if ( il->lifespan.died < gen )
    { if ( prev )
      { prev->older = older;
      } else
      { simpleMutexLock(&db->locks.misc);   /* sync with */
	p->is_leaf = older;		    /* is_leaf_predicate() */
	simpleMutexUnlock(&db->locks.misc);
      }

      memset(&il->lifespan, 0, sizeof(il->lifespan));
      deferred_free(&db->defer_clouds, il);
    } else
    { prev = il;
    }
  }
}


static void
free_is_leaf(rdf_db *db, predicate *p)
{ is_leaf *il, *older;

  for(il = p->is_leaf; il; il=older)
  { older = il->older;

    rdf_free(db, il, sizeof(*il));
  }

  p->is_leaf = NULL;
}


		 /*******************************
		 *   PRINT PREDICATE HIERARCHY	*
		 *******************************/

static int
check_predicate_cloud(predicate_cloud *c)
{ predicate **pp;
  int errors = 0;
  int i;

  for(i=0, pp=c->members; i<c->size; i++, pp++)
  { predicate *p = *pp;

    if ( p->label != i )
    { Sdprintf("Wrong label for %s (%d != %d\n", pname(p), i, p->label);
      errors++;
    }
    if ( p->hash != c->hash )
    { Sdprintf("Hash of %s doesn't match cloud hash\n", pname(p));
      errors++;				/* this is now normal! */
    }
    if ( p->cloud != c )
    { Sdprintf("Wrong cloud of %s\n", pname(p));
      errors++;
    }
  }

  return errors;
}


static void
print_reachability_cloud(rdf_db *db, predicate *p, int all)
{ int x, y;
  predicate_cloud *cloud = p->cloud;
  sub_p_matrix *rm;
  query *q;

  Sdprintf("Cloud has %d members, hash = 0x%x\n", cloud->size, cloud->hash);
  check_predicate_cloud(cloud);

  if ( !(q = open_query(db)) )
  { Sdprintf("No more open queries\n");
    return;
  }

  for(rm=cloud->reachable; rm; rm=rm->older)
  { char b[2][24];

    if ( !all && !alive_lifespan(q, &rm->lifespan) )
      continue;

    Sdprintf("\nReachability matrix: %s..%s (%s)\n  ",
	     gen_name(rm->lifespan.born, b[0]),
	     gen_name(rm->lifespan.died, b[1]),
	     alive_lifespan(q, &rm->lifespan) ? "alive" : "dead");

    for(x=0; x<rm->matrix->width; x++)
      Sdprintf("%d", x%10);
    Sdprintf("\n  ");
    for(y=0; y<rm->matrix->heigth; y++)
    { predicate *yp = cloud->members[y];

      for(x=0; x<rm->matrix->width; x++)
      { if ( testbit(rm->matrix, x, y) )
	  Sdprintf("X");
	else
	  Sdprintf(".");
      }

      if ( predicate_hash(yp) == cloud->hash )
	Sdprintf(" %2d %s\n  ", y, pname(yp));
      else
	Sdprintf(" %2d %s (hash=0x%x)\n  ", y, pname(yp), predicate_hash(yp));
      assert(cloud->members[y]->label == y);
    }
  }
  close_query(q);
}


static foreign_t
rdf_print_predicate_cloud(term_t t, term_t all)
{ predicate *p;
  rdf_db *db = rdf_current_db();
  int print_all;

  if ( !get_existing_predicate(db, t, &p) ||
       !PL_get_bool_ex(all, &print_all) )
    return FALSE;			/* error or no predicate */

  print_reachability_cloud(db, p, print_all);

  return TRUE;
}



/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Branching  factors  are  crucial  in  ordering    the  statements  of  a
conjunction. These functions compute  the   average  branching factor in
both directions ("subject --> P  -->  object"   and  "object  -->  P -->
subject") by determining the number of unique   values at either side of
the predicate. This number  is  only   recomputed  if  it  is considered
`dirty'.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
update_predicate_counts(rdf_db *db, predicate *p, int which, query *q)
{ size_t total = 0;

  if ( which == DISTINCT_DIRECT )
  { size_t changed;

    if ( p->triple_count >= p->distinct_updated[DISTINCT_DIRECT] )
      changed = p->triple_count - p->distinct_updated[DISTINCT_DIRECT];
    else
      changed = p->distinct_updated[DISTINCT_DIRECT] - p->triple_count;

    if ( changed < p->distinct_updated[DISTINCT_DIRECT] )
      return TRUE;

    if ( p->triple_count == 0 )
    { p->distinct_count[which]    = 0;
      p->distinct_subjects[which] = 0;
      p->distinct_objects[which]  = 0;

      return TRUE;
    }
  } else
  { size_t changed = db->queries.generation - p->distinct_updated[DISTINCT_SUB];

    if ( changed < p->distinct_count[DISTINCT_SUB] )
      return TRUE;
  }

  { atomset subject_set;
    atomset object_set;
    triple t;
    triple *byp;
    triple_walker tw;

    memset(&t, 0, sizeof(t));
    t.predicate.r = p;
    t.indexed |= BY_P;

    init_atomset(&subject_set);
    init_atomset(&object_set);
    init_triple_walker(&tw, db, &t, t.indexed);
    while((byp=next_triple(&tw)))
    { if ( byp->lifespan.died == GEN_MAX && !byp->is_duplicate )
      { if ( byp->predicate.r == p ||
	     (which != DISTINCT_DIRECT &&
	      isSubPropertyOf(db, byp->predicate.r, p, q)) )
	{ total++;
	  add_atomset(&subject_set, ID_ATOM(byp->subject_id));
	  add_atomset(&object_set, object_hash(byp)); /* NOTE: not exact! */
	}
      }
    }

    p->distinct_count[which]    = total;
    p->distinct_subjects[which] = subject_set.count;
    p->distinct_objects[which]  = object_set.count;

    destroy_atomset(&subject_set);
    destroy_atomset(&object_set);

    if ( which == DISTINCT_DIRECT )
      p->distinct_updated[DISTINCT_DIRECT] = total;
    else
      p->distinct_updated[DISTINCT_SUB] = db->queries.generation;

    DEBUG(1, Sdprintf("%s: distinct subjects (%s): %ld, objects: %ld\n",
		      PL_atom_chars(p->name),
		      (which == DISTINCT_DIRECT ? "rdf" : "rdfs"),
		      p->distinct_subjects[which],
		      p->distinct_objects[which]));
  }

  return TRUE;
}


static void
invalidate_distinct_counts(rdf_db *db)
{ int i;

  for(i=0; i<db->predicates.bucket_count; i++)
  { predicate *p = db->predicates.blocks[MSB(i)][i];

    for( ; p; p = p->next )
    { p->distinct_updated[DISTINCT_SUB] = 0;
      p->distinct_count[DISTINCT_SUB] = 0;
      p->distinct_subjects[DISTINCT_SUB] = 0;
      p->distinct_objects[DISTINCT_SUB] = 0;
    }
  }
}


static double
subject_branch_factor(rdf_db *db, predicate *p, query *q, int which)
{ if ( !update_predicate_counts(db, p, which, q) )
    return FALSE;

  if ( p->distinct_subjects[which] == 0 )
    return 0.0;				/* 0 --> 0 */

  return (double)p->distinct_count[which] /
         (double)p->distinct_subjects[which];
}


static double
object_branch_factor(rdf_db *db, predicate *p, query *q, int which)
{ if ( !update_predicate_counts(db, p, which, q) )
    return FALSE;

  if ( p->distinct_objects[which] == 0 )
    return 0.0;				/* 0 --> 0 */

  return (double)p->distinct_count[which] /
         (double)p->distinct_objects[which];
}




		 /*******************************
		 *	   NAMED GRAPHS		*
		 *******************************/

/* MT: all calls must be locked
*/

static int
init_graph_table(rdf_db *db)
{ size_t bytes = sizeof(graph**)*INITIAL_GRAPH_TABLE_SIZE;
  graph **p = PL_malloc_uncollectable(bytes);
  int i, count = INITIAL_GRAPH_TABLE_SIZE;

  memset(p, 0, bytes);
  for(i=0; i<MSB(count); i++)
    db->graphs.blocks[i] = p;

  db->graphs.bucket_count       = count;
  db->graphs.bucket_count_epoch = count;
  db->graphs.count              = 0;
  db->graphs.erased             = 0;

  return TRUE;
}


static int
resize_graph_table(rdf_db *db)
{ int i = MSB(db->graphs.bucket_count);
  size_t bytes  = sizeof(graph**)*db->graphs.bucket_count;
  graph **p = PL_malloc_uncollectable(bytes);

  memset(p, 0, bytes);
  db->graphs.blocks[i] = p-db->graphs.bucket_count;
  db->graphs.bucket_count *= 2;
  DEBUG(1, Sdprintf("Resized graph table to %ld\n",
		    (long)db->graphs.bucket_count));

  return TRUE;
}


typedef struct graph_walker
{ rdf_db       *db;			/* RDF DB */
  atom_t	name;			/* Name of the graph */
  size_t	unbounded_hash;		/* Atom's hash */
  size_t	bcount;			/* current bucket count */
  graph	       *current;		/* current location */
} graph_walker;


static void
init_graph_walker(graph_walker *gw, rdf_db *db, atom_t name)
{ gw->db	     = db;
  gw->name	     = name;
  gw->unbounded_hash = atom_hash(name, MURMUR_SEED);
  gw->bcount	     = db->graphs.bucket_count_epoch;
  gw->current	     = NULL;
}

static graph*
next_graph(graph_walker *gw)
{ graph *g;

  if ( gw->current )
  { g = gw->current;
    gw->current = g->next;
  } else if ( gw->bcount <= gw->db->graphs.bucket_count )
  { do
    { int entry = gw->unbounded_hash % gw->bcount;
      g = gw->db->graphs.blocks[MSB(entry)][entry];
      gw->bcount *= 2;
    } while(!g && gw->bcount <= gw->db->graphs.bucket_count );

    if ( g )
      gw->current = g->next;
  } else
    return NULL;

  return g;
}


static graph *
existing_graph(rdf_db *db, atom_t name)
{ graph_walker gw;
  graph *g;

  init_graph_walker(&gw, db, name);
  while((g=next_graph(&gw)))
  { if ( g->name == name )
      return g;
  }

  return g;
}


static graph *
lookup_graph(rdf_db *db, atom_t name)
{ graph *g, **gp;
  int entry;

  if ( (g=existing_graph(db, name)) && !g->erased )
    return g;

  LOCK_MISC(db);
  if ( (g=existing_graph(db, name)) )
  { if ( g->erased )
    { memset(g->digest,            0, sizeof(g->digest));
      memset(g->unmodified_digest, 0, sizeof(g->unmodified_digest));
      g->md5    = TRUE;
      g->erased = FALSE;
      db->graphs.erased--;
    }

    UNLOCK_MISC(db);
    return g;
  }

  g = rdf_malloc(db, sizeof(*g));
  memset(g, 0, sizeof(*g));
  g->name = name;
  g->md5 = TRUE;
  PL_register_atom(name);
  if ( db->graphs.count > db->graphs.bucket_count )
    resize_graph_table(db);
  entry = atom_hash(name, MURMUR_SEED) % db->graphs.bucket_count;
  gp = &db->graphs.blocks[MSB(entry)][entry];
  g->next = *gp;
  *gp = g;
  db->graphs.count++;
  UNLOCK_MISC(db);

  return g;
}


static void
erase_graphs(rdf_db *db)
{ int i;

  for(i=0; i<db->graphs.bucket_count; i++)
  { graph *n, *g = db->graphs.blocks[MSB(i)][i];

    db->graphs.blocks[MSB(i)][i] = NULL;

    for( ; g; g = n )
    { n = g->next;

      PL_unregister_atom(g->name);
      if ( g->source )
	PL_unregister_atom(g->source);
      rdf_free(db, g, sizeof(*g));
    }
  }

  db->graphs.count  = 0;
  db->graphs.erased = 0;
  db->last_graph    = NULL;
}


static int
gc_graphs(rdf_db *db, gen_t gen)
{ int reclaimed = 0;

  if ( db->graphs.erased > 10 + db->graphs.count/2 )
  { int i;

    LOCK_MISC(db);
    for(i=0; i<db->graphs.bucket_count; i++)
    { graph *p, *n, *g;

      p = NULL;
      g = db->graphs.blocks[MSB(i)][i];

      for( ; g; g = n )
      { n = g->next;

	if ( g->erased && g->triple_count == 0 )
	{ if ( p )
	    p->next = g->next;
	  else
	    db->graphs.blocks[MSB(i)][i] = g->next;

	  if ( db->last_graph == g )
	    db->last_graph = NULL;
	  db->graphs.count--;
	  db->graphs.erased--;
	  reclaimed++;
	  deferred_finalize(&db->defer_all, g,
			    finalize_graph, db);
	} else
	  p = g;
      }
    }
    UNLOCK_MISC(db);
  }

  return reclaimed;
}


static void
register_graph(rdf_db *db, triple *t)
{ graph *src;

  if ( !t->graph_id )
    return;

  if ( !((src=db->last_graph) && src->name == ID_ATOM(t->graph_id)) )
  { src = lookup_graph(db, ID_ATOM(t->graph_id));
    db->last_graph = src;
  }

  ATOMIC_ADD(&src->triple_count, 1);
#ifdef WITH_MD5
  if ( src->md5 )
  { md5_byte_t digest[16];
    md5_triple(t, digest);
    sum_digest(src->digest, digest);
  }
#endif
}


static void
unregister_graph(rdf_db *db, triple *t)
{ graph *src;

  if ( !t->graph_id )
    return;

  if ( db->last_graph && db->last_graph->name == ID_ATOM(t->graph_id) )
  { src = db->last_graph;
  } else
  { src = existing_graph(db, ID_ATOM(t->graph_id));
  }

  if ( src )
  { ATOMIC_SUB(&src->triple_count, 1);
#ifdef WITH_MD5
    if ( src->md5 )
    { md5_byte_t digest[16];
      md5_triple(t, digest);
      dec_digest(src->digest, digest);
    }
#endif
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
rdf_graph_(?Graph, ?TripleCount) is nondet.

True when Graph is a current graph with TripleCount triples.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

typedef struct enum_graph
{ graph *g;
  int i;
} enum_graph;


static graph *
advance_graph_enum(rdf_db *db, enum_graph *eg)
{ if ( eg->g )
    eg->g = eg->g->next;

  while ( !eg->g || (eg->g->erased && eg->g->triple_count == 0) )
  { if ( !eg->g )
    { while ( ++eg->i < db->graphs.bucket_count &&
	      !(eg->g = db->graphs.blocks[MSB(eg->i)][eg->i]) )
	;
      if ( !eg->g )
	return NULL;
    } else
      eg->g = eg->g->next;
  }

  return eg->g;
}


static foreign_t
rdf_graph(term_t name, term_t triple_count, control_t h)
{ rdf_db *db = rdf_current_db();
  enum_graph *eg;
  atom_t a;

  switch( PL_foreign_control(h) )
  { case PL_FIRST_CALL:
      if ( PL_is_variable(name) )
      { eg = rdf_malloc(db, sizeof(*eg));
	eg->i  = -1;
	eg->g  = NULL;
	advance_graph_enum(db, eg);
	goto next;
      } else if ( PL_get_atom_ex(name, &a) )
      { graph *g;

	if ( (g=existing_graph(db, a)) && !(g->erased && g->triple_count == 0) )
	  return PL_unify_int64(triple_count, g->triple_count);
      }
      return FALSE;
    case PL_REDO:
      eg = PL_foreign_context_address(h);
      goto next;
    case PL_PRUNED:
      eg = PL_foreign_context_address(h);
      rdf_free(db, eg, sizeof(*eg));
      return TRUE;
    default:
      assert(0);
      return FALSE;
  }

next:
  if ( !eg->g ||
       !PL_unify_atom(name, eg->g->name) ||
       !PL_unify_int64(triple_count, eg->g->triple_count) )
  { rdf_free(db, eg, sizeof(*eg));
    return FALSE;
  }

  if ( advance_graph_enum(db, eg) )
  { PL_retry_address(eg);
  } else
  { rdf_free(db, eg, sizeof(*eg));
    return TRUE;
  }
}


static foreign_t
rdf_graph_source(term_t graph_name, term_t source, term_t modified)
{ atom_t gn;
  rdf_db *db = rdf_current_db();

  if ( !get_atom_or_var_ex(graph_name, &gn) )
    return FALSE;

  if ( gn )
  { graph *s;

    if ( (s = existing_graph(db, gn)) &&
	 !(s->erased && s->triple_count == 0) &&
	 s->source)
    { return ( PL_unify_atom(source, s->source) &&
	       PL_unify_float(modified, s->modified) );
    }
  } else
  { atom_t src;

    if ( PL_get_atom_ex(source, &src) )
    { int i;

      for(i=0; i<db->graphs.bucket_count; i++)
      { graph *g = db->graphs.blocks[MSB(i)][i];

	for(; g; g=g->next)
	{ if ( g->source == src )
	  { return ( PL_unify_atom(graph_name, g->name) &&
		     PL_unify_float(modified, g->modified) );
	  }
	}
      }
    }
  }

  return FALSE;
}


static foreign_t
rdf_set_graph_source(term_t graph_name, term_t source, term_t modified)
{ atom_t gn, src;
  int rc = FALSE;
  rdf_db *db = rdf_current_db();
  graph *s;
  double mtime;

  if ( !PL_get_atom_ex(graph_name, &gn) ||
       !PL_get_atom_ex(source, &src) ||
       !PL_get_float_ex(modified, &mtime) )
    return FALSE;

  if ( (s = lookup_graph(db, gn)) )
  { LOCK_MISC(db);
    if ( s->source != src )
    { if ( s->source )
	PL_unregister_atom(s->source);
      s->source = src;
      PL_register_atom(s->source);
    }
    s->modified = mtime;
    UNLOCK_MISC(db);
    rc = TRUE;
  }

  return rc;
}


static foreign_t
rdf_create_graph(term_t graph_name)
{ atom_t gn;
  rdf_db *db = rdf_current_db();
  graph *g;

  if ( !PL_get_atom_ex(graph_name, &gn) )
    return FALSE;

  if ( (g = existing_graph(db, gn)) && !g->erased )
    return TRUE;				/* already exists */
  if ( (g = lookup_graph(db, gn)) )
  { rdf_broadcast(EV_CREATE_GRAPH, g, NULL);

    return TRUE;
  }

  return FALSE;
}


static void
clean_atom(atom_t *ap)
{ atom_t old;

  if ( (old=*ap) )
  { *ap = 0;
    PL_unregister_atom(old);
  }
}


static void
finalize_graph(void *mem, void *clientdata)
{ graph *g = mem;
  (void)clientdata;

  clean_atom(&g->name);
}


static foreign_t
rdf_destroy_graph(term_t graph_name)
{ atom_t gn;
  rdf_db *db = rdf_current_db();
  graph *g;

  if ( !PL_get_atom_ex(graph_name, &gn) )
    return FALSE;

  if ( (g = existing_graph(db, gn)) )
  { LOCK_MISC(db);
    g->md5 = FALSE;
    memset(g->digest,            0, sizeof(g->digest));
    memset(g->unmodified_digest, 0, sizeof(g->unmodified_digest));
    clean_atom(&g->source);
    g->modified = 0.0;
    g->erased = TRUE;
    db->graphs.erased++;
    if ( db->last_graph == g )
      db->last_graph = NULL;
    UNLOCK_MISC(db);
  }

  return TRUE;
}


#ifdef WITH_MD5
/** rdf_graph_modified_(+Graph, -IsModified, -UnmodifiedHash)

True when IsModified reflects  the  modified   status  relative  to  the
`unmodified' digest.
*/

static foreign_t
rdf_graph_modified_(term_t graph_name, term_t ismodified, term_t hash)
{ atom_t gn;
  rdf_db *db = rdf_current_db();
  graph *g;
  int rc;

  if ( !PL_get_atom_ex(graph_name, &gn) )
    return FALSE;

  if ( (g = lookup_graph(db, gn)) )
  { int ismod = (memcmp(g->digest, g->unmodified_digest, 16) != 0);

    rc = ( PL_unify_bool(ismodified, ismod) &&
	   md5_unify_digest(hash, g->unmodified_digest)
	 );
  } else
    rc = FALSE;

  return rc;
}


static int
clear_modified(graph *g)
{ if ( g->md5 )
  { memcpy(g->unmodified_digest, g->digest, 16);
    return TRUE;
  }

  return FALSE;
}


static foreign_t
rdf_graph_clear_modified_(term_t graph_name)
{ atom_t gn;
  rdf_db *db = rdf_current_db();
  graph *g;

  if ( !PL_get_atom_ex(graph_name, &gn) )
    return FALSE;

  if ( (g = lookup_graph(db, gn)) )
    return clear_modified(g);

  return FALSE;
}


#endif /*WITH_MD5*/


		 /*******************************
		 *	     LITERALS		*
		 *******************************/

static inline void
prepare_literal_ex(literal_ex *lex)
{
#ifdef LITERAL_EX_MAGIC
  lex->magic = LITERAL_EX_MAGIC;
#endif

  if ( lex->literal->objtype == OBJ_STRING )
  { lex->atom.handle = lex->literal->value.string;
    lex->atom.resolved = FALSE;
  }
}


static literal *
new_literal(rdf_db *db)
{ literal *lit = rdf_malloc(db, sizeof(*lit));
  memset(lit, 0, sizeof(*lit));
  lit->references = 1;

  return lit;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
free_literal_value() gets rid of atoms or term   that forms the value of
the literal. We cannot dispose of  these   immediately  as they might be
needed by an ongoing  scan  of   the  literal  skiplist  for comparison.
Therefore, we use deferred_finalize() and dispose of the triple later.

Return TRUE if the triple value  could   be  distroyed  and FALSE if the
destruction   has   been   deferred.   That     will   eventually   call
finalize_literal_ptr(), which calls free_literal_value()  again, but now
as not shared literal so it can do its work unconditionally.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void
finalize_literal_ptr(void *mem, void *clientdata)
{ literal **litp = mem;
  rdf_db *db = clientdata;
  literal *lit = *litp;

  free_literal_value(db, lit);
  rdf_free(db, lit, sizeof(*lit));
}


static literal **
unlink_literal(rdf_db *db, literal *lit)
{ if ( lit->shared && !db->resetting )
  { literal_ex lex;
    literal **data;

    lit->shared = FALSE;
    DEBUG(2,
	  Sdprintf("Delete %p from literal table: ", lit);
	  print_literal(lit);
	  Sdprintf("\n"));

    lex.literal = lit;
    prepare_literal_ex(&lex);

    if ( (data=skiplist_delete(&db->literals, &lex)) )
    { return data;
    } else
    { Sdprintf("Failed to delete %p (size=%ld): ", lit, db->literals.count);
      print_literal(lit);
      Sdprintf("\n");
      assert(0);
    }
  }

  return NULL;
}


static void
free_literal_value(rdf_db *db, literal *lit)
{ unlock_atoms_literal(lit);
  if ( lit->objtype == OBJ_TERM &&
       lit->value.term.record )
  { if ( lit->term_loaded )
      rdf_free(db, lit->value.term.record, lit->value.term.len);
    else
      PL_erase_external(lit->value.term.record);
  }
  lit->objtype = OBJ_UNTYPED;		/* debugging: trap errors early */
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
free_literal() frees a literal, normally referenced   from a triple. The
triple may be shared or not. Triples that   are part of the database are
always shared. Unshared  triples  are   typically  search  patterns,  or
created triples that are deleted  because   some  part  of the operation
fails.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
free_literal(rdf_db *db, literal *lit)
{ int rc = TRUE;

  if ( lit->shared )
  { simpleMutexLock(&db->locks.literal);
    if ( --lit->references == 0 )
    { literal **data = unlink_literal(db, lit);
      simpleMutexUnlock(&db->locks.literal);

      if ( data )			/* unlinked */
      { rc = rdf_broadcast(EV_OLD_LITERAL, lit, NULL);
	deferred_finalize(&db->defer_literals, data,
			  finalize_literal_ptr, db);
      } else
      { free_literal_value(db, lit);
	rdf_free(db, lit, sizeof(*lit));
      }
    } else
    { simpleMutexUnlock(&db->locks.literal);
    }
  } else				/* not shared; no locking needed */
  { if ( --lit->references == 0 )
    { free_literal_value(db, lit);
      rdf_free(db, lit, sizeof(*lit));
    }
  }

  return rc;
}


static literal *
copy_literal(rdf_db *db, literal *lit)
{ lit->references++;
  assert(lit->references != 0);
  return lit;
}


static void
alloc_literal_triple(rdf_db *db, triple *t)
{ if ( !t->object_is_literal )
  { t->object.literal = new_literal(db);
    t->object_is_literal = TRUE;
  }
}


static void
lock_atoms_literal(literal *lit)
{ if ( !lit->atoms_locked )
  { lit->atoms_locked = TRUE;

    switch(lit->objtype)
    { case OBJ_STRING:
	PL_register_atom(lit->value.string);
	if ( lit->qualifier )
	  PL_register_atom(ID_ATOM(lit->type_or_lang));
	break;
    }
  }
}


static void
unlock_atoms_literal(literal *lit)
{ if ( lit->atoms_locked )
  { lit->atoms_locked = FALSE;

    switch(lit->objtype)
    { case OBJ_STRING:
	PL_unregister_atom(lit->value.string);
	if ( lit->qualifier )
	  PL_unregister_atom(ID_ATOM(lit->type_or_lang));
	break;
    }
  }
}


		 /*******************************
		 *	     LITERAL DB		*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
compare_literals() sorts literals.  Ordering is defined as:

	* Numeric literals < string literals < term literals
	* Numeric literals (int and float) are sorted by value
	* String literals are sorted alhabetically
		- case independent, but uppercase before lowercase
		- locale (strcoll) sorting?
		- delete dyadrics
		- first on string, then on type, then on language
	* Terms are sorted on Prolog standard order of terms
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
cmp_qualifier(const literal *l1, const literal *l2)
{ if ( l1->qualifier != Q_NONE && l1->qualifier == l2->qualifier )
  { if ( l1->type_or_lang )
      return cmp_atoms(ID_ATOM(l1->type_or_lang), ID_ATOM(l2->type_or_lang));
    return -1;
  }

  return l1->qualifier - l2->qualifier;
}

static xsd_primary
is_numerical_string(const literal *lit)
{ if ( lit->objtype == OBJ_STRING &&
       lit->qualifier == Q_TYPE )
    return is_numeric_type(ID_ATOM(lit->type_or_lang));

  return XSD_NONNUMERIC;
}


static int
compare_literals(literal_ex *lex, literal *l2)
{ literal *l1 = lex->literal;

#ifdef LITERAL_EX_MAGIC
  assert(lex->magic == LITERAL_EX_MAGIC);
#endif

  if ( l1->objtype == l2->objtype )
  { int rc;

    switch(l1->objtype)
    { case OBJ_INTEGER:
      { int64_t v1 = l1->value.integer;
	int64_t v2 = l2->value.integer;
	rc = v1 < v2 ? -1 : v1 > v2 ? 1 : 0;
	break;
      }
      case OBJ_DOUBLE:
      { double v1 = l1->value.real;
	double v2 = l2->value.real;
	rc = v1 < v2 ? -1 : v1 > v2 ? 1 : 0;
	break;
      }
      case OBJ_STRING:
      { if ( lex->atom.handle == l2->value.string &&
	     l1->type_or_lang == l2->type_or_lang )
	{ rc = 0;
	} else
	{ xsd_primary nt1 = is_numerical_string(l1);
	  xsd_primary nt2 = is_numerical_string(l2);

	  if ( nt1 || nt2 )
	  { if ( nt1 && nt2 )
	    { rc = cmp_xsd_info(nt1, &lex->atom, nt2, l2->value.string);
	      if ( rc == 0 && nt1 != nt2 )
		rc = nt1 < nt2 ? 1 : -1;
	    } else
	    { rc = nt1 ? -1 : 1;
	    }
	  } else
	  { rc = cmp_atom_info(&lex->atom, l2->value.string);
	  }
	}
	break;
      }
      case OBJ_TERM:
      { fid_t fid = PL_open_foreign_frame();
	term_t t1 = PL_new_term_ref();
	term_t t2 = PL_new_term_ref();
					/* can also be handled in literal_ex */
	PL_recorded_external(l1->value.term.record, t1);
	PL_recorded_external(l2->value.term.record, t2);
	rc = PL_compare(t1, t2);

	PL_discard_foreign_frame(fid);
	break;
      }
      default:
	assert(0);
        return 0;
    }

    if ( rc != 0 )
      return rc;
    return cmp_qualifier(l1, l2);
  } else if ( l1->objtype == OBJ_INTEGER && l2->objtype == OBJ_DOUBLE )
  { double v1 = (double)l1->value.integer;
    double v2 = l2->value.real;
    return v1 < v2 ? -1 : v1 > v2 ? 1 : -1;
  } else if ( l1->objtype == OBJ_DOUBLE && l2->objtype == OBJ_INTEGER )
  { double v1 = l1->value.real;
    double v2 = (double)l2->value.integer;
    return v1 < v2 ? -1 : v1 > v2 ? 1 : 1;
  } else
  { return l1->objtype - l2->objtype;
  }
}

#ifdef SL_CHECK
static int sl_checking = FALSE;
#endif

static int
sl_compare_literals(void *p1, void *p2, void *cd)
{ literal *l2 = *(literal**)p2;
  (void)cd;

#ifdef SL_CHECK
  if ( sl_checking )
  { literal *l1 = *(literal**)p1;
    literal_ex lex;

    lex.literal = l1;
    prepare_literal_ex(&lex);
    return compare_literals(&lex, l2);
  } else
#endif
  { literal_ex *lex = p1;

    assert(l2->objtype != OBJ_UNTYPED);
    return compare_literals(lex, l2);
  }
}


#ifdef SL_CHECK
static int
sl_check(rdf_db *db, int print)
{ int rc = TRUE;

  DEBUG(2, { assert(sl_checking == FALSE);
	     sl_checking = TRUE;
	     rc = skiplist_check(&db->literals, print);
	     sl_checking = FALSE;
	   });

  return rc;
}
#else
#define sl_check(db, print) (void)0
#endif


static void *
sl_rdf_malloc(size_t bytes, void *cd)
{ return rdf_malloc(cd, bytes);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Create the sorted literal tree. Note  that   we  do  not register a free
handler  for  the  tree  as  nodes   are  either  already  destroyed  by
free_literal() or by rdf_reset_db().
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
init_literal_table(rdf_db *db)
{ skiplist_init(&db->literals,
		sizeof(literal*),	/* Payload size */
		db,			/* Client data */
		sl_compare_literals,	/* Compare */
		sl_rdf_malloc,		/* Allocate */
		NULL);			/* Destroy */

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
share_literal() takes a literal  and  replaces   it  with  one  from the
literal database if there is a match.   On a match, the argument literal
is destroyed. Without a match it adds   the  literal to the database and
returns it.

Called from add_triples() and update_triples() outside the locked areas.
We must hold db->locks.literal for updating the literal database.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static literal *
share_literal(rdf_db *db, literal *from)
{ literal **data, *shared;
  literal_ex lex;
  int is_new;
  static float existing = 0.0;
  static float new      = 0.0;

  if ( from->shared )
    return from;				/* already shared */

  lex.literal = from;
  prepare_literal_ex(&lex);

  if ( existing*2 > new &&
      (data = skiplist_find(&db->literals, &lex)) )
  { simpleMutexLock(&db->locks.literal);
    existing = existing*0.99+1.0;
    if ( !skiplist_erased_payload(&db->literals, data) )
    { shared = *data;
      shared->references++;
      assert(shared->references != 0);

      simpleMutexUnlock(&db->locks.literal);
      free_literal(db, from);

      return shared;
    }
    simpleMutexUnlock(&db->locks.literal);
  }

  simpleMutexLock(&db->locks.literal);
  sl_check(db, FALSE);
  data = skiplist_insert(&db->literals, &lex, &is_new);
  sl_check(db, FALSE);
  if ( is_new )
  { new = new*0.99+1.0;
    from->shared = TRUE;
    shared = from;
    assert(from->references==1);
    assert(from->atoms_locked==1);
  } else
  { existing = existing*0.99+1.0;
    shared = *data;
    shared->references++;
    assert(shared->references != 0);
  }
  simpleMutexUnlock(&db->locks.literal);

  if ( !is_new )
  { DEBUG(2,
	  Sdprintf("Replace %p by %p:\n", from, shared);
	  Sdprintf("\tfrom: "); print_literal(from);
	  Sdprintf("\n\tto: "); print_literal(shared);
	  Sdprintf("\n"));

    free_literal(db, from);
  } else
  { DEBUG(2,
	  Sdprintf("Insert %p into literal table: ", from);
	  print_literal(from);
	  Sdprintf("\n"));

    rdf_broadcast(EV_NEW_LITERAL, from, NULL);
  }

  return shared;
}


		 /*******************************
		 *	      TRIPLES		*
		 *******************************/

static triple *
alloc_triple(void)
{ triple *t = malloc(sizeof(*t));

  if ( t )
  { memset(t, 0, sizeof(*t));
#ifdef COMPACT
    t->id = TRIPLE_NO_ID;
#endif
  }

  return t;
}


static void
unalloc_triple(rdf_db *db, triple *t, int linger)
{ if ( t )
  { if ( linger )
    { TMAGIC(t, T_LINGERING);
#ifdef COMPACT
      if ( t->id != TRIPLE_NO_ID )
#endif
	deferred_finalize(&db->defer_triples, t,
			  finalize_triple, db);
      ATOMIC_ADD(&db->lingering, 1);
    } else
    { unlock_atoms(db, t);
      if ( t->object_is_literal && t->object.literal )
	free_literal(db, t->object.literal);
      SECURE(memset(t, 0, sizeof(*t)));
      TMAGIC(t, T_FREED);
      free(t);
    }
  }
}


		 /*******************************
		 *	    TRIPLE HASH		*
		 *******************************/

static int
init_triple_hash(rdf_db *db, int index, size_t count)
{ triple_hash *h = &db->hash[index];
  size_t bytes = sizeof(triple_bucket)*count;
  triple_bucket *t = PL_malloc_uncollectable(bytes);
  int i;

  memset(t, 0, bytes);
  memset(h, 0, sizeof(*h));

  h->optimize_threshold = col_opt_threshold[index];
  h->avg_chain_len      = col_avg_len[index];
  h->icol		= index;

  for(i=0; i<MSB(count); i++)
    h->blocks[i] = t;

  h->bucket_preinit = h->bucket_count_epoch = h->bucket_count = count;

  return TRUE;
}


static int
size_triple_hash(rdf_db *db, int index, size_t size)
{ triple_hash *hash = &db->hash[index];
  int extra;

  if ( hash->created )
    rdf_create_gc_thread(db);

  simpleMutexLock(&db->queries.write.lock);
  extra = MSB(size) - MSB(hash->bucket_count);
  while( extra-- > 0 )
  { int i = MSB(hash->bucket_count);
    size_t bytes  = sizeof(triple_bucket)*hash->bucket_count;
    triple_bucket *t = PL_malloc_uncollectable(bytes);

    memset(t, 0, bytes);
    hash->blocks[i] = t-hash->bucket_count;
    hash->bucket_count *= 2;
    if ( !hash->created )
      hash->bucket_count_epoch = hash->bucket_count;
    DEBUG(1, Sdprintf("Resized triple index %s=%d to %ld at %d\n",
		      col_name[index], index, (long)hash->bucket_count, i));
  }
  simpleMutexUnlock(&db->queries.write.lock);

  return TRUE;
}


static void
reset_triple_hash(rdf_db *db, triple_hash *hash)
{ size_t bytes = sizeof(triple_bucket)*hash->bucket_preinit;
  int i;

  memset(hash->blocks[0], 0, bytes);	/* clear first block */
  for(i=MSB(hash->bucket_preinit); i<MAX_TBLOCKS; i++)
  { if ( hash->blocks[i] )
    { triple_bucket *t = hash->blocks[i];

      hash->blocks[i] = NULL;
      t += 1<<(i-1);
      PL_free(t);
    } else
      break;
  }
  hash->bucket_count = hash->bucket_count_epoch = hash->bucket_preinit;
  hash->created = FALSE;
}


/* count_different() returns the number of elements in a hash bucket
   that have a different unbounded hash.  That is, the bucket might
   split if we resize the table.

   *count is assigned with the size.  That is merely consistency because
   we also keep track of this value.
*/

#define COUNT_DIFF_NOHASH 5

static int
count_different(rdf_db *db, triple_bucket *tb, int index, int *count)
{ triple *t;
  int rc;

  if ( tb->count < COUNT_DIFF_NOHASH )
  { if ( tb->count <= 1 )
    { *count = tb->count;

      return tb->count;
    } else
    { size_t hashes[COUNT_DIFF_NOHASH];
      int different = 0;
      int found = 0;

      for(t = fetch_triple(db, tb->head);
	  t && different < COUNT_DIFF_NOHASH;	/* be careful with concurrently */
	  t = triple_follow_hash(db, t, ICOL(index))) /* added triples */
      { size_t hash = triple_hash_key(t, index);
	int i;

	found++;
	for(i=0; i<different; i++)
	{ if ( hashes[i] == hash )
	    goto next;
	}
	hashes[different++] = hash;

      next:;
      }

      *count = found;

      return different;
    }
  } else
  { atomset hash_set;
    int c = 0;

    init_atomset(&hash_set);
    for(t=fetch_triple(db, tb->head); t; t=triple_follow_hash(db, t, ICOL(index)))
    { c++;
      add_atomset(&hash_set, (atom_t)triple_hash_key(t, index));
    }
    rc = hash_set.count;
    destroy_atomset(&hash_set);

    *count = c;
  }

  return rc;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
triple_hash_quality() computes the quality of the triple hash index. The
return is 1.0 if the unbounded hashkey for all objects in each bucket is
the same, and < 1.0 if there  are buckets holding objects with different
unbounded keys.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static float
triple_hash_quality(rdf_db *db, int index, int sample)
{ triple_hash *hash = &db->hash[index];
  int i, step;
  float q = 0;
  size_t total = 0;

  if ( index == 0 )
    return 1.0;

  if ( sample > 0 )
    step = (hash->bucket_count+sample)/sample;	/* step >= 1 */
  else
    step = 1;

  for(i=0; i<hash->bucket_count; i += step)
  { int entry = MSB(i);
    triple_bucket *tb = &hash->blocks[entry][i];
    int count;
    int different = count_different(db, tb, col_index[index], &count);

    DEBUG(1,			/* inconsistency is normal due to concurrency */
	  if ( count != tb->count )
	    Sdprintf("Inconsistent count in index=%d, bucket=%d, %d != %d\n",
		     index, i, count, tb->count));

    if ( count )
    { q += (float)count/(float)different;
      total += count;
    }
  }

  return total == 0 ? 1.0 : q/(float)total;
}


#ifdef O_DEBUG
void
print_triple_hash(rdf_db *db, int index, int sample)
{ triple_hash *hash = &db->hash[index];
  int i, step;

  if ( sample > 0 )
    step = (hash->bucket_count+sample)/sample;	/* step >= 1 */
  else
    step = 1;

  for(i=0; i<hash->bucket_count; i += step)
  { int entry = MSB(i);
    triple_bucket *tb = &hash->blocks[entry][i];
    int count;
    int different = count_different(db, tb, col_index[index], &count);

    if ( count != 0 )
    { triple *t;

      Sdprintf("%d: c=%d; d=%d", i, count, different);
      for(t=fetch_triple(db, tb->head); t; t=triple_follow_hash(db, t, index))
      { Sdprintf("\n\t");
	print_triple(t, 0);
      }
    }
  }
}
#endif /*O_DEBUG*/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Consider resizing the hash-tables. The argument 'extra' gives the number
of triples that  will  be  added.  This   is  used  to  guess  the  hash
requirements of the table  and  thus   avoid  duplicating  triples in on
optimize_triple_hashes().
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

void
consider_triple_rehash(rdf_db *db, size_t extra)
{ size_t triples = db->created - db->erased;
  triple_hash *spo = &db->hash[ICOL(BY_SPO)];

  if ( (extra + triples)/spo->avg_chain_len > spo->bucket_count )
  { int i;
    int resized = 0;
    int factor = ((extra+triples+100000)*16)/(triples+100000);

#define SCALE(n) (((n)*factor)/(16*db->hash[i].avg_chain_len))
#define SCALEF(n) (((n)*(float)factor)/(16.0*(float)db->hash[i].avg_chain_len))

    for(i=1; i<INDEX_TABLES; i++)
    { int resize = 0;
      size_t sizenow = db->hash[i].bucket_count;

      if ( db->hash[i].user_size || db->hash[i].created == FALSE )
	continue;			/* user set size */

      switch(col_index[i])
      { case BY_S:
	case BY_SG:
	case BY_SP:
	  while ( SCALE(db->resources.hash.count) > sizenow<<resize )
	    resize++;
	  break;
	case BY_P:
	  while ( SCALE(db->predicates.count) > sizenow<<resize )
	    resize++;
	  break;
	case BY_O:
	case BY_PO:
	{ size_t setsize = SCALE(db->resources.hash.count + db->literals.count);

	  if ( setsize > triples )
	    setsize = triples;
	  while ( setsize > sizenow<<resize )
	    resize++;
	  break;
	}
	case BY_SPO:
	  while ( (extra+triples)/spo->avg_chain_len > sizenow<<resize )
	    resize++;
	  break;
	case BY_G:
	  while ( SCALE(db->graphs.count) > sizenow<<resize )
	    resize++;
	  break;
	case BY_PG:
	{ size_t s;

	  s = (db->graphs.count < db->predicates.count ?
				  db->predicates.count : db->graphs.count);

	  while ( SCALE(s) > sizenow<<resize )
	    resize++;
	  break;
	}
	default:
	  assert(0);
      }

      if ( resize )
      { resized++;
	size_triple_hash(db, i, sizenow<<resize);
      }
    }

#undef SCALE
#undef SCALEF

    if ( resized )
      invalidate_distinct_counts(db);
  }
}


static size_t
distinct_hash_values(rdf_db *db, int icol)
{ triple *t;
  size_t count;
  atomset hash_set;
  int byx = col_index[icol];

  init_atomset(&hash_set);
  for(t=fetch_triple(db, db->by_none.head);
      t;
      t=triple_follow_hash(db, t, ICOL(BY_NONE)))
  { add_atomset(&hash_set, (atom_t)triple_hash_key(t, byx));
  }
  count = hash_set.count;
  destroy_atomset(&hash_set);

  return count;
}


static void
initial_size_triple_hash(rdf_db *db, int icol)
{ triple_hash *hash = &db->hash[icol];
  size_t size;

  switch(col_index[icol])
  { case BY_S:
      size = db->resources.hash.count;
      break;
    case BY_P:
      size = db->predicates.count;
      break;
    case BY_O:
      size = db->resources.hash.count + db->literals.count;
      break;
    case BY_SPO:
      size = db->created - db->erased;
      break;
    case BY_G:
      size = db->graphs.count;
      break;
    case BY_PO:
    case BY_SG:
    case BY_SP:
    case BY_PG:
      size = distinct_hash_values(db, icol);
      break;
    default:
      assert(0);
      return;
  }

  size /= hash->avg_chain_len;
  size_triple_hash(db, icol, size);
}


static int
init_tables(rdf_db *db)
{ int ic;
  triple_hash *by_none = &db->hash[ICOL(BY_NONE)];

  by_none->blocks[0] = &db->by_none;
  by_none->bucket_count_epoch = 1;
  by_none->bucket_count = 1;
  by_none->created = TRUE;

  for(ic=BY_S; ic<INDEX_TABLES; ic++)
  { if ( !init_triple_hash(db, ic, INITIAL_TABLE_SIZE) )
      return FALSE;
  }

  return (init_resource_db(db, &db->resources) &&
	  init_pred_table(db) &&
	  init_graph_table(db) &&
	  init_literal_table(db));
}


		 /*******************************
		 *     INDEX OPTIMIZATION	*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Index optimization copies triples  that  have   been  indexed  while the
hash-table was small to the  current  table.   This  adds  a copy of the
triple to the index (at the new place).   The  old triple gets a pointer
->reindexed pointing to the new version.   deref_triple() finds the real
triple.

The next thing we need to do is   reclaim this in gc_hash_chain(). To to
that, we replace old->lifespan.died with db->reindexed++. The logic that
finds old queries  also  finds  the   query  with  the  oldest reindexed
counter. Triples that have yet older   old->lifespan.died  can safely be
removed.

TBD: To preserve order, we must insert   the  new triples before the old
ones. This is significantly more complex,   notably because they must be
re-indexed in reverse order in  this  case.   Probably  the  best way to
implement this is to collect the  triples   that  must be reindexed in a
triple buffer and then use a version of link_triple_hash() that prepends
the triples, calling on the triples from the buffer in reverse order. We
will ignore this for now: triple ordering has no semantics.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void
reindex_triple(rdf_db *db, triple *t)
{ triple *t2 = alloc_triple();

  *t2 = *t;
  t2->has_reindex_prev = TRUE;
  memset(&t2->tp, 0, sizeof(t2->tp));
  register_triple(db, t2);
  simpleMutexLock(&db->queries.write.lock);
  link_triple_hash(db, t2);
  TMAGIC(t2, T_CHAINED2);
  t->reindexed = T_ID(t2);
  TMAGIC(t, T_REINDEXED);
  t->lifespan.died = db->reindexed++;
  if ( t2->object_is_literal )			/* do not deallocate lit twice */
  { simpleMutexLock(&db->locks.literal);
    t2->object.literal->references++;
    assert(t2->object.literal->references != 0);
    simpleMutexUnlock(&db->locks.literal);
  }
  t->atoms_locked = FALSE;			/* same for unlock_atoms() */
  simpleMutexUnlock(&db->queries.write.lock);
}


static int
optimizable_triple_hash(rdf_db *db, int icol)
{ triple_hash *hash = &db->hash[icol];
  int opt = 0;
  size_t epoch;

  if ( hash->created == FALSE )
    return FALSE;

  for ( epoch=hash->bucket_count_epoch; epoch < hash->bucket_count; epoch*=2 )
    opt++;

  opt -= hash->optimize_threshold;
  if ( opt < 0 )
    opt = 0;

  return opt;
}


static int
optimizable_hashes(rdf_db *db)
{ int icol;
  int optimizable = 0;

  for(icol=1; icol<INDEX_TABLES; icol++)
    optimizable += optimizable_triple_hash(db, icol);

  return optimizable;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
optimize_triple_hash() only doubles hash->bucket_count_epoch!  It may be
necessary to call it multiple times, but   reindexing one step at a time
is not slower than doing it all at once (is this true?)

Note that there is another reason  to  do   only  a  little  of the work
because copying the triples temporarily costs memory.

(*) We have already done the reindexing  from another index. It may also
mean that this triple was reindexed in a  previous pass, but that GC has
not yet reclaimed the triple. I think that  should be fine because it is
old and burried anyway, but still accessible for old queries.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
optimize_triple_hash(rdf_db *db, int icol, gen_t gen)
{ triple_hash *hash = &db->hash[icol];

  if ( optimizable_triple_hash(db, icol) )
  { size_t b_no = 0;
    size_t upto = hash->bucket_count_epoch;
    size_t copied = 0;

    for( ; b_no < upto; b_no++ )
    { triple_bucket *bucket = &hash->blocks[MSB(b_no)][b_no];
      triple *t;

      for(t=fetch_triple(db, bucket->head); t; t=triple_follow_hash(db, t, icol))
      { if ( t->lifespan.died >= gen &&
	     !t->reindexed &&		/* see (*) */
	     triple_hash_key(t, col_index[icol]) % hash->bucket_count != b_no )
	{ reindex_triple(db, t);
	  copied++;
	}
      }
    }

    hash->bucket_count_epoch = upto*2;
    DEBUG(1, Sdprintf("Optimized hash %s (epoch=%ld; size=%ld; copied=%ld)\n",
		      col_name[icol],
		      (long)hash->bucket_count_epoch,
		      (long)hash->bucket_count,
		      (long)copied));

    return 1;
  }

  return 0;
}


static int
optimize_triple_hashes(rdf_db *db, gen_t gen)
{ int icol;
  int optimized = 0;

  for(icol=1; icol<INDEX_TABLES; icol++)
  { enter_scan(&db->defer_all);
    optimized += optimize_triple_hash(db, icol, gen);
    exit_scan(&db->defer_all);
    if ( PL_handle_signals() < 0 )
      return -1;
  }

  return optimized;			/* # hashes optimized */
}


		 /*******************************
		 *	GARBAGE COLLECTION	*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Garbage collect triples, given that the   oldest  running query reads at
generation gen.	 There are two thing we can do:

  - Remove any triple that died before gen.  These triples must be left
    to GC.  See also alloc.c.

We count `uncollectable' triples: erased triples that still have queries
that depend on them. If no  such  triples   exist  there  is no point in
running GC.

Should do something similar with reindexed   triples  that cannot yet be
collected? The problem is less likely,   because they become ready after
all active _queries_ started before the reindexing have died, wereas the
generation stuff depends on longer lived  objects which as snapshots and
transactions.

t->linked is managed at three placed:   link_triple_hash(), where we are
sure that the triple is not garbage  (are we, reindex_triple()?), when a
new index is created and when the triple has been removed from the index
links (below).
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static inline int
is_garbage_triple(triple *t, gen_t old_query_gen, gen_t old_reindex_gen)
{ if ( t->has_reindex_prev )
    return FALSE;

  if ( t->reindexed )				/* Safe: reindex_triple() */
    return t->lifespan.died < old_reindex_gen;	/* is also part of GC */
  else
    return t->lifespan.died < old_query_gen;
}


static size_t
gc_hash_chain(rdf_db *db, size_t bucket_no, int icol,
	      gen_t gen, gen_t reindex_gen)
{ triple_bucket *bucket = &db->hash[icol].blocks[MSB(bucket_no)][bucket_no];
  triple *prev = NULL;
  triple *t;
  size_t collected = 0;
  size_t uncollectable = 0;

  for(t = fetch_triple(db, bucket->head); t; t=triple_follow_hash(db, t, icol))
  { if ( is_garbage_triple(t, gen, reindex_gen) )
    { simpleMutexLock(&db->queries.write.lock);

      if ( prev )
	prev->tp.next[icol] = t->tp.next[icol];
      else
	bucket->head = t->tp.next[icol];
      if ( T_ID(t) == bucket->tail )
	bucket->tail = T_ID(prev);

      collected++;

      if ( --t->linked == 0 )
      { DEBUG(2, { char buf[2][64];
		   Sdprintf("GC at gen=%s..%s: ",
			    gen_name(t->lifespan.born, buf[0]),
			    gen_name(t->lifespan.died, buf[1]));
		   print_triple(t, PRT_NL);
		 });

	if ( t->reindexed )
	{ triple *t2 = fetch_triple(db, t->reindexed);

	  db->gc.reclaimed_reindexed++;
	  t2->has_reindex_prev = FALSE;
	} else
	  db->gc.reclaimed_triples++;

	simpleMutexUnlock(&db->queries.write.lock);
	free_triple(db, t, TRUE);
      } else
      { simpleMutexUnlock(&db->queries.write.lock);
      }
    } else
    { prev=t;
      if ( icol == 0 && t->erased && !t->reindexed &&
	   t->lifespan.died >= gen )
	uncollectable++;
    }
  }

  if ( collected && icol > 0 )		/* concurrent with hashing new ones */
    ATOMIC_SUB(&bucket->count, collected);

  if ( icol == 0 )
  { char buf[64];

    DEBUG(4, Sdprintf("At %s: %lld uncollectable\n",
		      gen_name(gen, buf),
		      uncollectable));
    db->gc.uncollectable = uncollectable;
  }

  return collected;
}


static size_t
gc_hash(rdf_db *db, int icol, gen_t gen, gen_t reindex_gen)
{ size_t mb = db->hash[icol].bucket_count;
  size_t b;
  size_t collected = 0;

  for(b=0; b<mb; b++)
    collected += gc_hash_chain(db, b, icol, gen, reindex_gen);

  return collected;
}


static int
gc_hashes(rdf_db *db, gen_t gen, gen_t reindex_gen)
{ size_t garbage = db->erased    - db->gc.reclaimed_triples;
  size_t reindex = db->reindexed - db->gc.reclaimed_reindexed;

  if ( garbage + reindex > 0 )
  { int icol;

    for(icol=0; icol<INDEX_TABLES; icol++)
    { size_t collected;

      if ( db->hash[icol].created )
      { enter_scan(&db->defer_all);
	collected = gc_hash(db, icol, gen, reindex_gen);
	exit_scan(&db->defer_all);

	if ( PL_handle_signals() < 0 )
	  return -1;
      } else
	collected = 0;

      if ( icol == 0 && collected == 0 )
	break;
    }
  }

  return 0;
}


static int
gc_set_busy(rdf_db *db)
{ int busy;

  simpleMutexLock(&db->locks.misc);
  if ( !(busy = db->gc.busy) )
    db->gc.busy = TRUE;
  simpleMutexUnlock(&db->locks.misc);

  return !busy;
}


static void
gc_clear_busy(rdf_db *db)
{ simpleMutexLock(&db->locks.misc);
  db->gc.busy = FALSE;
  simpleMutexUnlock(&db->locks.misc);
}


static int
gc_db(rdf_db *db, gen_t gen, gen_t reindex_gen)
{ char buf[64];
  int rc;

  if ( !gc_set_busy(db) )
    return FALSE;
  simpleMutexLock(&db->locks.gc);
  DEBUG(10, Sdprintf("RDF GC; gen = %s\n", gen_name(gen, buf)));
  if ( optimize_triple_hashes(db, gen) >= 0 &&
       gc_hashes(db, gen, reindex_gen) >= 0 &&
       gc_clouds(db, gen) >= 0 &&
       gc_graphs(db, gen) >= 0 )
  { db->gc.count++;
    db->gc.last_gen = gen;
    db->gc.last_reindex_gen = reindex_gen;
    rc = TRUE;
  } else
    rc = FALSE;
  gc_clear_busy(db);
  simpleMutexUnlock(&db->locks.gc);

  return rc;
}


static int
suspend_gc(rdf_db *db)
{ int was_busy = db->gc.busy;

  DEBUG(2, if ( was_busy )
	     Sdprintf("Reset: GC in progress, waiting ...\n"));

  simpleMutexLock(&db->locks.gc);
  DEBUG(2, if ( was_busy )
	     Sdprintf("Reset: GC finished\n"));
  db->gc.busy		     = TRUE;
  db->gc.count		     = 0;
  db->gc.time		     = 0.0;
  db->gc.reclaimed_triples   = 0;
  db->gc.reclaimed_reindexed = 0;
  db->reindexed		     = 0;
  db->gc.uncollectable	     = 0;
  db->gc.last_gen	     = 0;
  db->gc.busy		     = FALSE;

  return TRUE;
}


static void
resume_gc(rdf_db *db)
{ simpleMutexUnlock(&db->locks.gc);
}



/** rdf_gc_(-Done) is semidet.

Run the RDF-DB garbage collector. The collector   is  typically ran in a
seperate thread. Its execution does not  interfere with readers and only
synchronizes with writers using short-held locks.

Fails without any action if there is already a GC in progress.
*/

static foreign_t
rdf_gc(void)
{ rdf_db *db = rdf_current_db();
  gen_t reindex_gen;
  gen_t gen = oldest_query_geneneration(db, &reindex_gen);

  return gc_db(db, gen, reindex_gen);
}


/** rdf_add_gc_time(+Time:double) is det.

Add CPU time to GC statistics.  This is left to Prolog

*/

static foreign_t
rdf_add_gc_time(term_t time)
{ double t;

  if ( PL_get_float_ex(time, &t) )
  { rdf_db *db = rdf_current_db();

    db->gc.time += t;
    return TRUE;
  }

  return FALSE;
}

/** rdf_gc_info(-Info) is det.

Return info to help deciding on whether or not to call rdf_gc. Info is a
record with the following members:

  1. Total number of triples in hash (dead or alive)
  2. Total dead triples in hash (deleted or reindexed)
  3. Total reindexed but not reclaimed triples
  4. Total number of possible optimizations to hash-tables.
  5. Oldest generation we must keep
  6. Oldest generation at last GC
  7. Oldest reindexed triple we must keep
  8. Oldest reindexed at last GC
*/

#define INT_ARG(val) PL_INT64, (int64_t)(val)

static foreign_t
rdf_gc_info(term_t info)
{ rdf_db *db     = rdf_current_db();
  size_t life    = db->created   - db->gc.reclaimed_triples;
  size_t garbage = db->erased    - db->gc.reclaimed_triples;
  size_t reindex = db->reindexed - db->gc.reclaimed_reindexed;
  gen_t keep_reindex;
  gen_t keep_gen = oldest_query_geneneration(db, &keep_reindex);

  if ( keep_gen == db->gc.last_gen )
  { garbage -= db->gc.uncollectable;
    assert((int64_t)garbage >= 0);
  }

  return PL_unify_term(info,
		       PL_FUNCTOR_CHARS, "gc_info", 8,
		         INT_ARG(life),
		         INT_ARG(garbage),
		         INT_ARG(reindex),
		         INT_ARG(optimizable_hashes(db)),
		         INT_ARG(keep_gen),
		         INT_ARG(db->gc.last_gen),
		         INT_ARG(keep_reindex),
		         INT_ARG(db->gc.last_reindex_gen));
}


		 /*******************************
		 *	      GC THREAD		*
		 *******************************/

int
rdf_create_gc_thread(rdf_db *db)
{ if ( db->gc.thread_started )
    return TRUE;

  simpleMutexLock(&db->locks.misc);
  if ( !db->gc.thread_started )
  { db->gc.thread_started = TRUE;

    PL_call_predicate(NULL, PL_Q_NORMAL,
		      PL_predicate("rdf_create_gc_thread", 0, "rdf_db"), 0);
  }
  simpleMutexUnlock(&db->locks.misc);

  return TRUE;
}


		 /*******************************
		 *	  OVERALL DATABASE	*
		 *******************************/

static rdf_db *
new_db(void)
{ rdf_db *db = PL_malloc_uncollectable(sizeof(*db));

  memset(db, 0, sizeof(*db));
  INIT_LOCK(db);
  init_tables(db);
  init_triple_array(db);
  init_query_admin(db);
  db->prefixes = new_prefix_table();

  db->duplicate_admin_threshold = DUPLICATE_ADMIN_THRESHOLD;
  db->snapshots.keep = GEN_MAX;
  db->queries.generation = GEN_EPOCH;

  return db;
}


static rdf_db *RDF_DB;

rdf_db *
rdf_current_db(void)
{ if ( RDF_DB )
    return RDF_DB;

  simpleMutexLock(&rdf_lock);
  if ( !RDF_DB )
    RDF_DB = new_db();
  simpleMutexUnlock(&rdf_lock);

  return RDF_DB;
}


static triple *
new_triple(rdf_db *db)
{ triple *t = alloc_triple();
  t->allocated = TRUE;

  return t;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
free_triple() is called in  two  scenarios.   One  is  from  the garbage
collector after a triple is deleted from   all hash chains. In this case
the linger argument is TRUE and  the   next-pointers  of the triples are
still in place because search may be   scanning  the triple. See alloc.c
for details on the triple memory management. The second case is deletion
of temporary triples, something that may   happen  from many threads. In
either case, this is typically called unlocked.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void
free_triple(rdf_db *db, triple *t, int linger)
{ if ( t->match == STR_MATCH_BETWEEN )
    free_literal_value(db, &t->tp.end);

  if ( !t->allocated )
  { unlock_atoms(db, t);
    if ( t->object_is_literal && t->object.literal )
    { free_literal(db, t->object.literal);
      t->object_is_literal = FALSE;
    }
  } else
  { unalloc_triple(db, t, linger);
  }
}


static size_t
literal_hash(literal *lit)
{ if ( lit->hash )
  { return lit->hash;
  } else
  { unsigned int hash;

    switch(lit->objtype)
    { case OBJ_STRING:
	hash = atom_hash_case(lit->value.string);
        break;
      case OBJ_INTEGER:
      case OBJ_DOUBLE:
	hash = rdf_murmer_hash(&lit->value.integer,
			       sizeof(lit->value.integer),
			       MURMUR_SEED);
        break;
      case OBJ_TERM:
	hash = rdf_murmer_hash(lit->value.term.record,
			       (int)lit->value.term.len,
			       MURMUR_SEED);
	break;
      default:
	assert(0);
	return 0;
    }

    if ( !hash )
      hash = 0x1;			/* cannot be 0 */

    lit->hash = hash;
    return lit->hash;
  }
}


static size_t
object_hash(triple *t)
{ if ( t->object_is_literal )
  { return literal_hash(t->object.literal);
  } else
  { return atom_hash(t->object.resource, OBJ_MURMUR_SEED);
  }
}


static size_t
subject_hash(triple *t)
{ return atom_hash(t->subject_id, SUBJ_MURMUR_SEED);
}

static size_t
graph_hash(triple *t)
{ return atom_hash(t->graph_id, GRAPH_MURMUR_SEED);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
triple_hash_key() computes the hash for a triple   on  a given index. It
can only be called for indices defined in the col_index-array. Note that
the returned value is unconstrained and  needs   to  be taken modulo the
table-size.

If   you   change   anything   here,   you    might   need   to   update
init_cursor_from_literal().
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static size_t
triple_hash_key(triple *t, int which)
{ size_t v = 0;

  assert(t->resolve_pred == FALSE);

  if ( which&BY_S ) v ^= subject_hash(t);
  if ( which&BY_P ) v ^= predicate_hash(t->predicate.r);
  if ( which&BY_O ) v ^= object_hash(t);
  if ( which&BY_G ) v ^= graph_hash(t);

  return v;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
by_inverse[] returns the index key to use   for inverse search as needed
to realise symmetric and inverse predicates.

Note that this only deals with the   non-G(graph)  indices because it is
only used by rdf_has/3 and rdf_reachable/3.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int by_inverse[8] =
{ BY_NONE,				/* BY_NONE = 0 */
  BY_O,					/* BY_S    = 1 */
  BY_P,					/* BY_P    = 2 */
  BY_PO,				/* BY_SP   = 3 */
  BY_S,					/* BY_O    = 4 */
  BY_SO,				/* BY_SO   = 5 */
  BY_SP,				/* BY_PO   = 6 */
  BY_SPO,				/* BY_SPO  = 7 */
};


static inline void
append_triple_bucket(rdf_db *db, triple_bucket *bucket, int icol, triple *t)
{ if ( bucket->tail )
  { fetch_triple(db, bucket->tail)->tp.next[icol] = T_ID(t);
  } else
  { bucket->head = T_ID(t);
  }
  bucket->tail = T_ID(t);
  ATOMIC_INC(&bucket->count);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
(*) ->linked is decremented in gc_hash_chain() for garbage triples. This
can conflict. We must use some sort   of  synchronization with GC if the
died generation is not the maximum and the triple might thus be garbage.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void
create_triple_hashes(rdf_db *db, int count, int *ic)
{ triple_hash *hashes[16];
  int i, mx=0;

  for(i=0; i<count; i++)
  { hashes[mx] = &db->hash[ic[i]];
    if ( !hashes[mx]->created )
    { initial_size_triple_hash(db, hashes[mx]->icol);
      mx++;
    }
  }
  hashes[mx] = NULL;

  if ( mx > 0 )
  { simpleMutexLock(&db->queries.write.lock);

    for(i=0; i<mx; i++)
    { if ( hashes[i]->created )
      { mx--;
	memmove(&hashes[i], &hashes[i+1], sizeof(hashes[0])*(mx-i));
      } else
      { DEBUG(1, Sdprintf("Creating hash %s\n", col_name[hashes[i]->icol]));
      }
    }

    if ( mx > 0 )
    { triple *t;

      for(t=fetch_triple(db, db->by_none.head);
	  t;
	  t=triple_follow_hash(db, t, ICOL(BY_NONE)))
      { for(i=0; i<mx; i++)
	{ triple_hash *hash = hashes[i];
	  int i = col_index[hash->icol];
	  int key = triple_hash_key(t, i) % hash->bucket_count;
	  triple_bucket *bucket = &hash->blocks[MSB(key)][key];

	  append_triple_bucket(db, bucket, hash->icol, t);
	  t->linked++;				/* (*) atomic? */
	}
      }

      for(i=0; i<mx; i++)
      { triple_hash *hash = hashes[i];
	hash->created = TRUE;
      }
    }
    simpleMutexUnlock(&db->queries.write.lock);
  }
}


/* called with queries.write.lock held */

static void
link_triple_hash(rdf_db *db, triple *t)
{ int ic;
  int linked = 1;

  append_triple_bucket(db, &db->by_none, ICOL(BY_NONE), t);

  for(ic=1; ic<INDEX_TABLES; ic++)
  { triple_hash *hash = &db->hash[ic];

    if ( hash->created )
    { int i = col_index[ic];
      int key = triple_hash_key(t, i) % hash->bucket_count;
      triple_bucket *bucket = &hash->blocks[MSB(key)][key];

      append_triple_bucket(db, bucket, ic, t);
      linked++;
    }
  }

  t->linked = linked;				/* safe: never garbage */
}


/* prelink_triple() performs that part of the triple loading that does
   not require locking.
*/

int
prelink_triple(rdf_db *db, triple *t, query *q)
{ register_triple(db, t);
  if ( t->resolve_pred )
  { t->predicate.r = lookup_predicate(db, t->predicate.u);
    t->resolve_pred = FALSE;
  }
  if ( t->object_is_literal )
    t->object.literal = share_literal(db, t->object.literal);
  if ( db->maintain_duplicates )
    mark_duplicate(db, t, q);

  return TRUE;
}


/* MT: Caller must be hold db->queries.write.lock

   Return: FALSE if nothing changed; TRUE if the database has changed
   TBD: Not all of this requires locking.  Most should be moved out of
   the lock:

	- Check for duplicates (?)
	- Consider re-hash
	- subProperty admin
*/

void
add_triple_consequences(rdf_db *db, triple *t, query *q)
{ if ( t->predicate.r->name == ATOM_subPropertyOf &&
       t->object_is_literal == FALSE )
  { addSubPropertyOf(db, t, q);
  }
}


/* Called with queries.write.lock held */

int
link_triple(rdf_db *db, triple *t, query *q)
{ assert(!t->linked);

  link_triple_hash(db, t);
  TMAGIC(t, T_CHAINED1);
  add_triple_consequences(db, t, q);
  db->created++;

  return TRUE;
}


int
postlink_triple(rdf_db *db, triple *t, query *q)
{ register_predicate(db, t);
  register_graph(db, t);

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Erase a triple from the DB.

MT: Caller must be hold db->queries.write.lock
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

void
del_triple_consequences(rdf_db *db, triple *t, query *q)
{ if ( t->predicate.r->name == ATOM_subPropertyOf &&
       t->object_is_literal == FALSE )
    delSubPropertyOf(db, t, q);
}


void
erase_triple(rdf_db *db, triple *t, query *q)
{ if ( t->erased )
    return;

  simpleMutexLock(&db->locks.erase);
  if ( !t->erased )
  { db->erased++;			/* incr. must be before setting erased */
    t->erased = TRUE;			/* to make sure #garbage >= 0 */
    simpleMutexUnlock(&db->locks.erase);

    unregister_graph(db, t);		/* Updates count and MD5 */
    unregister_predicate(db, t);	/* Updates count */
    if ( t->is_duplicate )
      ATOMIC_SUB(&db->duplicates, 1);
  } else
  { simpleMutexUnlock(&db->locks.erase);
  }
}


static int
match_literals(int how, literal *p, literal *e, literal *v)
{ literal_ex lex;

  lex.literal = p;
  prepare_literal_ex(&lex);

  DEBUG(2, { Sdprintf("match_literals(");
	     print_literal(p);
	     Sdprintf(", ");
	     print_literal(v);
	     Sdprintf(")\n"); });

  switch(how)
  { case STR_MATCH_LT:
      return compare_literals(&lex, v) > 0;
    case STR_MATCH_LE:
      return compare_literals(&lex, v) >= 0;
    case STR_MATCH_EQ:
      return compare_literals(&lex, v) == 0;
    case STR_MATCH_GE:
      return compare_literals(&lex, v) <= 0;
    case STR_MATCH_GT:
      return compare_literals(&lex, v) < 0;
    case STR_MATCH_BETWEEN:
      if ( compare_literals(&lex, v) <= 0 )
      { lex.literal = e;
	prepare_literal_ex(&lex);

	if ( compare_literals(&lex, v) >= 0 )
	  return TRUE;
      }
      return FALSE;
    default:
      return match_atoms(how, p->value.string, v->value.string);
  }
}


static int
match_numerical(int how, literal *p, literal *e, literal *v)
{ xsd_primary nv, np;
  literal_ex lex;

  if ( !(nv=is_numerical_string(v)) )
    return FALSE;
  if ( !p->value.string )		/* literal(eq(type(<numeric>,_)),_) */
    return TRUE;

  np = is_numerical_string(p);
  assert(np);

  lex.literal = p;
  prepare_literal_ex(&lex);

  switch(how)
  { case STR_MATCH_LT:
      return cmp_xsd_info(np, &lex.atom, nv, v->value.string)  > 0;
    case STR_MATCH_LE:
      return cmp_xsd_info(np, &lex.atom, nv, v->value.string) >= 0;
    case STR_MATCH_GE:
      return cmp_xsd_info(np, &lex.atom, nv, v->value.string) <= 0;
    case STR_MATCH_GT:
      return cmp_xsd_info(np, &lex.atom, nv, v->value.string) <  0;
    case STR_MATCH_BETWEEN:
      if ( cmp_xsd_info(np, &lex.atom, nv, v->value.string) <= 0 )
      { lex.literal = e;
	prepare_literal_ex(&lex);

	if ( cmp_xsd_info(np, &lex.atom, nv, v->value.string) >= 0 )
	  return TRUE;
      }
      return FALSE;
    case STR_MATCH_EQ:
    default:
      return cmp_xsd_info(np, &lex.atom, nv, v->value.string) == 0;
  }
}


static int
match_object(triple *t, triple *p, unsigned flags)
{ if ( p->object_is_literal )
  { if ( t->object_is_literal )
    { literal *plit = p->object.literal;
      literal *tlit = t->object.literal;

      if ( !plit->objtype && !plit->qualifier )
	return TRUE;

      if ( plit->objtype && plit->objtype != tlit->objtype )
	return FALSE;

      switch( plit->objtype )
      { case 0:
	  if ( plit->type_or_lang == ATOM_ID(ATOM_xsdString) &&
	       tlit->qualifier == Q_NONE )
	    return TRUE;
	  if ( plit->qualifier &&
	       tlit->qualifier != plit->qualifier )
	    return FALSE;
	  if ( plit->type_or_lang &&
	       tlit->type_or_lang != plit->type_or_lang )
	    return FALSE;
	  return TRUE;
	case OBJ_STRING:
	  /* numeric match */
	  if ( (flags&MATCH_NUMERIC) )
	    return match_numerical(p->match, plit, &p->tp.end, tlit);
	  /* qualifier match */
	  if ( !( plit->type_or_lang == ATOM_ID(ATOM_xsdString) &&
		  tlit->qualifier == Q_NONE ) )
	  { if ( (flags & MATCH_QUAL) ||
		 p->match == STR_MATCH_PLAIN )
	    { if ( tlit->qualifier != plit->qualifier )
		return FALSE;
	    } else
	    { if ( plit->qualifier && tlit->qualifier &&
		   tlit->qualifier != plit->qualifier )
		return FALSE;
	    }
	    if ( plit->type_or_lang &&
		 tlit->type_or_lang != plit->type_or_lang )
	      return FALSE;
	  }
	  /* lexical match */
	  if ( plit->value.string )
	  { if ( tlit->value.string != plit->value.string ||
		 p->match == STR_MATCH_LT || p->match == STR_MATCH_GT )
	    { if ( p->match >= STR_MATCH_ICASE )
	      { return match_literals(p->match, plit, &p->tp.end, tlit);
	      } else
	      { return FALSE;
	      }
	    }
	  }
	  return TRUE;
	case OBJ_INTEGER:
	  if ( p->match >= STR_MATCH_LT )
	    return match_literals(p->match, plit, &p->tp.end, tlit);
	  return tlit->value.integer == plit->value.integer;
	case OBJ_DOUBLE:
	  if ( p->match >= STR_MATCH_LT )
	    return match_literals(p->match, plit, &p->tp.end, tlit);
	  return tlit->value.real == plit->value.real;
	case OBJ_TERM:
	  if ( p->match >= STR_MATCH_LT )
	    return match_literals(p->match, plit, &p->tp.end, tlit);
	  if ( plit->value.term.record &&
	       plit->value.term.len != tlit->value.term.len )
	    return FALSE;
	  return memcmp(tlit->value.term.record, plit->value.term.record,
			plit->value.term.len) == 0;
	default:
	  assert(0);
      }
    }
    return FALSE;
  } else
  { if ( p->object.resource )
    { if ( t->object_is_literal ||
	   (p->object.resource != t->object.resource) )
	return FALSE;
    }
  }

  return TRUE;
}



/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
match_triples() is TRUE if the triple  t   matches  the  pattern p. This
function does not  consider  whether  or   not  the  triple  is visible.
Matching is controlled by flags:

    - MATCH_SUBPROPERTY		Perform rdfs:subPropertyOf matching
    - MATCH_SRC			Also match the source
    - MATCH_QUAL		Match language/type qualifiers
    - STR_MATCH_*		Additional string matching
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
match_triples(rdf_db *db, triple *t, triple *p, query *q, unsigned flags)
{ /* DEBUG(3, Sdprintf("match_triple(");
	   print_triple(t, 0);
	   Sdprintf(")\n"));
  */

  if ( p->subject_id && t->subject_id != p->subject_id )
    return FALSE;
  if ( !match_object(t, p, flags) )
    return FALSE;
  if ( flags & MATCH_SRC )
  { if ( p->graph_id && t->graph_id != p->graph_id )
      return FALSE;
    if ( p->line && t->line != p->line )
      return FALSE;
  }
					/* last; may be expensive */
  if ( p->predicate.r && t->predicate.r != p->predicate.r )
  { if ( (flags & MATCH_SUBPROPERTY) )
      return isSubPropertyOf(db, t->predicate.r, p->predicate.r, q);
    else
      return FALSE;
  }
  return TRUE;
}


		 /*******************************
		 *	      SAVE/LOAD		*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
The RDF triple format.  This format is intended for quick save and load
and not for readability or exchange.  Parts are based on the SWI-Prolog
Quick Load Format (implemented in pl-wic.c).

	<file>		::= <magic>
			    <version>
			    ['S' <graph-name>]
			    ['F' <graph-source>]
		            ['t' <modified>]
			    ['M' <md5>]
			    {<triple>}
			    'E'

	<magic>		::= "RDF-dump\n"
	<version>	::= <integer>

	<md5>		::= <byte>*		(16 bytes digest)

	<triple>	::= 'T'
	                    <subject>
			    <predicate>
			    <object>
			    <graph>

	<subject>	::= <resource>
	<predicate>	::= <resource>

	<object>	::= "R" <resource>
			  | "L" <atom>

	<resource>	::= <atom>

	<atom>		::= "X" <integer>
			    "A" <string>
			    "W" <utf-8 string>

	<string>	::= <integer><bytes>

	<graph-name>	::= <atom>
	<graph-source>	::= <atom>

	<graph>	::= <graph-file>
			    <line>
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#define SAVE_MAGIC "RDF-dump\n"
#define SAVE_VERSION 2

typedef struct saved
{ union
  { atom_t     atom;
    predicate *pred;
    literal   *lit;
    void      *any;
  } value;
  size_t as;
  struct saved *next;
} saved;


typedef struct saved_table
{ saved ** saved_table;
  size_t   saved_size;
  size_t   saved_id;
  tmp_store *store;
} saved_table;


static inline int
saved_hash(void *value, unsigned int seed)
{ return rdf_murmer_hash(&value, sizeof(value), seed);
}


static void
init_saved_table(rdf_db *db, saved_table *tab, tmp_store *store)
{ size_t size = 64;
  size_t bytes = size * sizeof(*tab->saved_table);

  tab->saved_table = rdf_malloc(db, bytes);
  memset(tab->saved_table, 0, bytes);
  tab->saved_size = size;
  tab->saved_id = 0;
  tab->store = store;
}

static void
resize_saved(rdf_db *db, saved_table *tab)
{ size_t newsize = tab->saved_size * 2;
  size_t newbytes = sizeof(*tab->saved_table) * newsize;
  saved **newt = rdf_malloc(db, newbytes);
  saved **s = tab->saved_table;
  int i;

  memset(newt, 0, newbytes);
  for(i=0; i<tab->saved_size; i++, s++)
  { saved *c, *n;

    for(c=*s; c; c = n)
    { int hash = saved_hash(c->value.any, MURMUR_SEED) % newsize;

      n = c->next;
      c->next = newt[hash];
      newt[hash] = c;
    }
  }

  rdf_free(db, tab->saved_table, tab->saved_size*sizeof(*tab->saved_table));
  tab->saved_table = newt;
  tab->saved_size  = newsize;
}


static void
destroy_saved_table(rdf_db *db, saved_table *tab)
{ if ( tab->saved_table )
    rdf_free(db, tab->saved_table, tab->saved_size*sizeof(*tab->saved_table));
}

static saved *
lookup_saved(saved_table *tab, void *value)
{ int hash = saved_hash(value, MURMUR_SEED) % tab->saved_size;
  saved *s;

  for(s=tab->saved_table[hash]; s; s= s->next)
  { if ( s->value.any == value )
      return s;
  }

  return NULL;
}

static saved *
add_saved(rdf_db *db, saved_table *tab, void *value)
{ int hash;
  saved *s;

  if ( tab->saved_id/4 > tab->saved_size )
    resize_saved(db, tab);

  hash = saved_hash(value, MURMUR_SEED) % tab->saved_size;
  if ( (s = alloc_tmp_store(tab->store, sizeof(*s))) )
  { s->value.any = value;
    s->as = tab->saved_id++;
    s->next = tab->saved_table[hash];
    tab->saved_table[hash] = s;
  }

  return s;
}


typedef struct save_context
{ saved_table	atoms;
  saved_table	literals;
  saved_table	predicates;
  tmp_store	store;
  int		version;			/* current save version */
} save_context;

static void
init_saved(rdf_db *db, save_context *ctx, int version)
{ init_tmp_store(&ctx->store);
  init_saved_table(db, &ctx->atoms, &ctx->store);
  if ( version > 2 )
  { init_saved_table(db, &ctx->literals, &ctx->store);
    init_saved_table(db, &ctx->predicates, &ctx->store);
  }
  ctx->version = version;
}

static void
destroy_saved(rdf_db *db, save_context *ctx)
{ destroy_saved_table(db, &ctx->atoms);
  if ( ctx->version > 2 )
  { destroy_saved_table(db, &ctx->literals);
    destroy_saved_table(db, &ctx->predicates);
  }
  destroy_tmp_store(&ctx->store);
}

static saved *
lookup_saved_atom(save_context *ctx, atom_t a)
{ return lookup_saved(&ctx->atoms, (void*)a);
}

static saved *
add_saved_atom(rdf_db *db, save_context *ctx, atom_t a)
{ return add_saved(db, &ctx->atoms, (void*)a);
}

static saved *
lookup_saved_literal(save_context *ctx, literal *l)
{ return lookup_saved(&ctx->literals, l);
}

static saved *
add_saved_literal(rdf_db *db, save_context *ctx, literal *l)
{ return add_saved(db, &ctx->literals, l);
}

static saved *
lookup_saved_predicate(save_context *ctx, predicate *p)
{ return lookup_saved(&ctx->predicates, p);
}

static saved *
add_saved_predicate(rdf_db *db, save_context *ctx, predicate *p)
{ return add_saved(db, &ctx->predicates, p);
}


#define INT64BITSIZE (sizeof(int64_t)*8)
#define PLMINLONG   ((int64_t)((uint64_t)1<<(INT64BITSIZE-1)))

static void
save_int(IOSTREAM *fd, int64_t n)
{ int m;
  int64_t absn = (n >= 0 ? n : -n);

  if ( n != PLMINLONG )
  { if ( absn < ((intptr_t)1 << 5) )
    { Sputc((int)(n & 0x3f), fd);
      return;
    } else if ( absn < ((intptr_t)1 << 13) )
    { Sputc((int)(((n >> 8) & 0x3f) | (1 << 6)), fd);
      Sputc((int)(n & 0xff), fd);
      return;
    } else if ( absn < ((intptr_t)1 << 21) )
    { Sputc((int)(((n >> 16) & 0x3f) | (2 << 6)), fd);
      Sputc((int)((n >> 8) & 0xff), fd);
      Sputc((int)(n & 0xff), fd);
      return;
    }
  }

  for(m = sizeof(n); ; m--)
  { int b = (int)((absn >> (((m-1)*8)-1)) & 0x1ff);

    if ( b == 0 )
      continue;
    break;
  }

  Sputc(m | (3 << 6), fd);

  for( ; m > 0; m--)
  { int b = (int)((n >> ((m-1)*8)) & 0xff);

    Sputc(b, fd);
  }
}


#define BYTES_PER_DOUBLE sizeof(double)
#ifdef WORDS_BIGENDIAN
static const int double_byte_order[] = { 7,6,5,4,3,2,1,0 };
#else
static const int double_byte_order[] = { 0,1,2,3,4,5,6,7 };
#endif

static int
save_double(IOSTREAM *fd, double f)
{ unsigned char *cl = (unsigned char *)&f;
  unsigned int i;

  for(i=0; i<BYTES_PER_DOUBLE; i++)
    Sputc(cl[double_byte_order[i]], fd);

  return TRUE;
}


static int
save_atom(rdf_db *db, IOSTREAM *out, atom_t a, save_context *ctx)
{ saved *s;
  size_t len;
  const char *chars;
  unsigned int i;
  const wchar_t *wchars;

  if ( (s=lookup_saved_atom(ctx, a)) )
  { Sputc('X', out);
    save_int(out, s->as);

    return TRUE;
  } else
  { s = add_saved_atom(db, ctx, a);
  }

  if ( (chars = PL_atom_nchars(a, &len)) )
  { Sputc('A', out);
    save_int(out, len);
    for(i=0; i<len; i++, chars++)
      Sputc(*chars&0xff, out);
  } else if ( (wchars = PL_atom_wchars(a, &len)) )
  { IOENC enc = out->encoding;

    Sputc('W', out);
    save_int(out, len);
    out->encoding = ENC_UTF8;
    for(i=0; i<len; i++, wchars++)
    { wint_t c = *wchars;

      SECURE(assert(c>=0 && c <= 0x10ffff));
      Sputcode(c, out);
    }
    out->encoding = enc;
  } else
    return FALSE;

  return TRUE;
}


static int
save_predicate(rdf_db *db, IOSTREAM *out, predicate *p, save_context *ctx)
{ if ( ctx->version > 2 )
  { saved *s;

    if ( (s=lookup_saved_predicate(ctx, p)) )
    { Sputc('X', out);
      save_int(out, s->as);

      return TRUE;
    } else
    { s = add_saved_predicate(db, ctx, p);
      Sputc('P', out);
    }
  }

  return save_atom(db, out, p->name, ctx);
}

static int
save_literal(rdf_db *db, IOSTREAM *out, literal *lit, save_context *ctx)
{ if ( ctx->version > 2 )
  { saved *s;

    if ( (s=lookup_saved_literal(ctx, lit)) )
    { Sputc('X', out);
      save_int(out, s->as);

      return TRUE;
    } else
    { s = add_saved_literal(db, ctx, lit);
    }
  }

  if ( lit->qualifier )
  { assert(lit->type_or_lang);
    Sputc(lit->qualifier == Q_LANG ? 'l' : 't', out);
    save_atom(db, out, ID_ATOM(lit->type_or_lang), ctx);
  }

  switch(lit->objtype)
  { case OBJ_STRING:
      Sputc('L', out);
      save_atom(db, out, lit->value.string, ctx);
      break;
    case OBJ_INTEGER:
      Sputc('I', out);
      save_int(out, lit->value.integer);
      break;
    case OBJ_DOUBLE:
    {	Sputc('F', out);
      save_double(out, lit->value.real);
      break;
    }
    case OBJ_TERM:
    { const char *s = lit->value.term.record;
      size_t len = lit->value.term.len;

      Sputc('T', out);
      save_int(out, len);
      while(len-- > 0)
	Sputc(*s++, out);

      break;
    }
    default:
      assert(0);
  }

  return TRUE;
}



static void
write_triple(rdf_db *db, IOSTREAM *out, triple *t, save_context *ctx)
{ Sputc('T', out);

  save_atom(db, out, ID_ATOM(t->subject_id), ctx);
  save_predicate(db, out, t->predicate.r, ctx);

  if ( t->object_is_literal )
  { save_literal(db, out, t->object.literal, ctx);
  } else
  { Sputc('R', out);
    save_atom(db, out, t->object.resource, ctx);
  }

  save_atom(db, out, ID_ATOM(t->graph_id), ctx);
  save_int(out, t->line);
}


static void
write_source(rdf_db *db, IOSTREAM *out, atom_t src, save_context *ctx)
{ graph *s = existing_graph(db, src);

  if ( s && s->source )
  { Sputc('F', out);
    save_atom(db, out, s->source, ctx);
    Sputc('t', out);
    save_double(out, s->modified);
  }
}


static void
write_md5(rdf_db *db, IOSTREAM *out, atom_t src)
{ graph *s = existing_graph(db, src);

  if ( s )
  { md5_byte_t *p = s->digest;
    int i;

    Sputc('M', out);
    for(i=0; i<16; i++)
      Sputc(*p++, out);
  }
}


static int
save_db(query *q, IOSTREAM *out, atom_t src, int version)
{ rdf_db *db = q->db;
  triple *t, p;
  save_context ctx;
  triple_walker tw;

  memset(&p, 0, sizeof(p));
  init_saved(db, &ctx, version);

  Sfprintf(out, "%s", SAVE_MAGIC);
  save_int(out, version);
  if ( src )
  { Sputc('S', out);			/* start of graph header */
    save_atom(db, out, src, &ctx);
    write_source(db, out, src, &ctx);
    write_md5(db, out, src);
    p.graph_id = ATOM_ID(src);
    p.indexed = BY_G;
  } else
  { p.indexed = BY_NONE;
  }
  if ( Sferror(out) )
    return FALSE;

  init_triple_walker(&tw, db, &p, p.indexed);
  while((t=next_triple(&tw)))
  { triple *t2;

    if ( (t2=alive_triple(q, t)) &&
	 (!src || ID_ATOM(t2->graph_id) == src) )
    { write_triple(db, out, t2, &ctx);
      if ( Sferror(out) )
	return FALSE;
    }
  }
  Sputc('E', out);
  if ( Sferror(out) )
    return FALSE;

  destroy_saved(db, &ctx);

  return TRUE;
}


static foreign_t
rdf_save_db(term_t stream, term_t graph, term_t version)
{ rdf_db *db = rdf_current_db();
  query *q;
  IOSTREAM *out;
  atom_t src;
  int rc;
  int v;

  if ( !PL_get_stream_handle(stream, &out) )
    return PL_type_error("stream", stream);
  if ( !get_atom_or_var_ex(graph, &src) )
    return FALSE;
  if ( !PL_get_integer(version, &v) )
    return FALSE;
  if ( v < 2 || v > 3 )
    return PL_domain_error("rdf_db_save_version", version);

  if ( (q = open_query(db)) )
  { rc = save_db(q, out, src, v);
    close_query(q);
    return rc;
  } else
    return FALSE;
}


static int64_t
load_int(IOSTREAM *fd)
{ int64_t first = Sgetc(fd);
  int bytes, shift, b;

  if ( !(first & 0xc0) )		/* 99% of them: speed up a bit */
  { first <<= (INT64BITSIZE-6);
    first >>= (INT64BITSIZE-6);

    return first;
  }

  bytes = (int) ((first >> 6) & 0x3);
  first &= 0x3f;

  if ( bytes <= 2 )
  { for( b = 0; b < bytes; b++ )
    { first <<= 8;
      first |= Sgetc(fd) & 0xff;
    }

    shift = (sizeof(first)-1-bytes)*8 + 2;
  } else
  { int m;

    bytes = (int)first;
    first = 0L;

    for(m=0; m<bytes; m++)
    { first <<= 8;
      first |= Sgetc(fd) & 0xff;
    }
    shift = (sizeof(first)-bytes)*8;
  }

  first <<= shift;
  first >>= shift;

  return first;
}


static int
load_double(IOSTREAM *fd, double *fp)
{ double f;
  unsigned char *cl = (unsigned char *)&f;
  unsigned int i;

  for(i=0; i<BYTES_PER_DOUBLE; i++)
  { int c = Sgetc(fd);

    if ( c == -1 )
    { *fp = 0.0;
      return FALSE;
    }
    cl[double_byte_order[i]] = c;
  }

  *fp = f;
  return TRUE;
}


typedef struct ld_array
{ size_t	loaded_id;
  size_t	allocated_size;
  void	      **loaded_objects;
} ld_array;

typedef struct ld_context
{ ld_array	atoms;
  ld_array	predicates;
  ld_array	literals;
  atom_t	graph_name;		/* for single-graph files */
  graph	       *graph;
  atom_t	graph_source;
  double	modified;
  int		has_digest;
  int		version;
  md5_byte_t    digest[16];
  atomset       graph_table;		/* multi-graph file */
  triple_buffer	triples;
} ld_context;


static int
add_object(rdf_db *db, void *obj, ld_array *ar)
{ if ( ar->loaded_id >= ar->allocated_size )
  { if ( ar->allocated_size == 0 )
    { ar->allocated_size = 1024;
      ar->loaded_objects = malloc(sizeof(void*)*ar->allocated_size);
    } else
    { size_t  bytes;
      void *new;

      ar->allocated_size *= 2;
      bytes = sizeof(void*)*ar->allocated_size;
      if ( (new = realloc(ar->loaded_objects, bytes)) )
	ar->loaded_objects = new;
      else
	return FALSE;
    }
  }

  ar->loaded_objects[ar->loaded_id++] = obj;
  return TRUE;
}

static int
add_atom(rdf_db *db, atom_t a, ld_context *ctx)
{ return add_object(db, (void*)a, &ctx->atoms);
}

static atom_t
fetch_atom(ld_context *ctx, size_t idx)
{ if ( idx < ctx->atoms.loaded_id )
    return (atom_t)ctx->atoms.loaded_objects[idx];

  return (atom_t)0;
}

static atom_t
load_atom(rdf_db *db, IOSTREAM *in, ld_context *ctx)
{ switch(Sgetc(in))
  { case 'X':
    { size_t idx = (size_t)load_int(in);
      return fetch_atom(ctx, idx);
    }
    case 'A':
    { size_t len = (size_t)load_int(in);
      atom_t a;

      if ( len < 1024 )
      { char buf[1024];
	Sfread(buf, 1, len, in);
	a = PL_new_atom_nchars(len, buf);
      } else
      { char *buf = rdf_malloc(db, len);
	Sfread(buf, 1, len, in);
	a = PL_new_atom_nchars(len, buf);
	rdf_free(db, buf, len);
      }

      add_atom(db, a, ctx);
      return a;
    }
    case 'W':
    { int len = (int)load_int(in);
      atom_t a;
      wchar_t buf[1024];
      wchar_t *w;
      IOENC enc = in->encoding;
      int i;

      if ( len < 1024 )
	w = buf;
      else
	w = rdf_malloc(db, len*sizeof(wchar_t));

      in->encoding = ENC_UTF8;
      for(i=0; i<len; i++)
      { w[i] = Sgetcode(in);
	SECURE(assert(w[i]>=0 && w[i] <= 0x10ffff));
      }
      in->encoding = enc;

      a = PL_new_atom_wchars(len, w);
      if ( w != buf )
	rdf_free(db, w, len*sizeof(wchar_t));

      add_atom(db, a, ctx);
      return a;
    }
    default:
    { assert(0);
      return 0;
    }
  }
}


static int
add_predicate(rdf_db *db, predicate *p, ld_context *ctx)
{ return add_object(db, p, &ctx->predicates);
}

static predicate *
fetch_predicate(ld_context *ctx, size_t idx)
{ if ( idx < ctx->predicates.loaded_id )
    return ctx->predicates.loaded_objects[idx];

  return NULL;
}

static predicate *
load_predicate(rdf_db *db, IOSTREAM *in, ld_context *ctx)
{ switch(Sgetc(in))
  { case 'X':
    { size_t idx = (size_t)load_int(in);
      return fetch_predicate(ctx, idx);
    }
    case 'P':
    { atom_t a;

      if ( (a=load_atom(db, in, ctx)) )
      { predicate *p;

	if ( (p=lookup_predicate(db, a)) &&
	     add_predicate(db, p, ctx) )
	  return p;
      }
      return NULL;			/* no memory */
    }
    default:
      assert(0);
      return NULL;
  }
}


static int
add_literal(rdf_db *db, literal *lit, ld_context *ctx)
{ return add_object(db, lit, &ctx->literals);
}

static literal *
fetch_literal(ld_context *ctx, size_t idx)
{ if ( idx < ctx->literals.loaded_id )
    return ctx->literals.loaded_objects[idx];

  return NULL;
}

static literal *
load_literal(rdf_db *db, IOSTREAM *in, ld_context *ctx, int c)
{ literal *lit;

  if ( c == 'X' && ctx->version >= 3 )
  { size_t idx = (size_t)load_int(in);
    lit = fetch_literal(ctx, idx);
    simpleMutexLock(&db->locks.literal);
    lit->references++;
    assert(lit->references != 0);
    simpleMutexUnlock(&db->locks.literal);
  } else if ( (lit=new_literal(db)) )
  {
  value:
    switch(c)
    { case 'L':
	lit->objtype = OBJ_STRING;
	lit->value.string = load_atom(db, in, ctx);
	break;
      case 'I':
	lit->objtype = OBJ_INTEGER;
	lit->value.integer = load_int(in);
	break;
      case 'F':
	lit->objtype = OBJ_DOUBLE;
        load_double(in, &lit->value.real);
	break;
      case 'T':
      { unsigned int i;
	char *s;

	lit->objtype = OBJ_TERM;
	lit->value.term.len = (size_t)load_int(in);
	lit->value.term.record = rdf_malloc(db, lit->value.term.len);
	lit->term_loaded = TRUE;	/* see free_literal() */
	s = (char *)lit->value.term.record;

	for(i=0; i<lit->value.term.len; i++)
	  s[i] = Sgetc(in);

	break;
      }
      case 'l':
	lit->qualifier = Q_LANG;
	lit->type_or_lang = ATOM_ID(load_atom(db, in, ctx));
	c = Sgetc(in);
	goto value;
      case 't':
	lit->qualifier = Q_TYPE;
	lit->type_or_lang = ATOM_ID(load_atom(db, in, ctx));
	c = Sgetc(in);
	goto value;
      default:
	assert(0);
        return NULL;
    }

    if ( ctx->version >= 3 )
    { lock_atoms_literal(lit);
      lit = share_literal(db, lit);

      add_literal(db, lit, ctx);
    }
  }

  return lit;
}


static triple *
load_triple(rdf_db *db, IOSTREAM *in, ld_context *ctx)
{ triple *t = new_triple(db);
  int c;

  t->subject_id = ATOM_ID(load_atom(db, in, ctx));
  if ( ctx->version < 3 )
  { t->resolve_pred = TRUE;
    t->predicate.u = load_atom(db, in, ctx);
  } else
  { t->predicate.r = load_predicate(db, in, ctx);
  }
  if ( (c=Sgetc(in)) == 'R' )
  { t->object.resource = load_atom(db, in, ctx);
  } else
  { t->object_is_literal = TRUE;
    t->object.literal = load_literal(db, in, ctx, c);
  }
  t->graph_id = ATOM_ID(load_atom(db, in, ctx));
  t->line  = (unsigned long)load_int(in);
  if ( !ctx->graph )
    add_atomset(&ctx->graph_table, ID_ATOM(t->graph_id));

  return t;
}


static int
load_magic(IOSTREAM *in)
{ char *s = SAVE_MAGIC;

  for( ; *s; s++)
  { if ( Sgetc(in) != *s )
      return FALSE;
  }

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Note that we have two types  of   saved  states.  One holding many named
graphs and one holding the content of exactly one named graph.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
load_db(rdf_db *db, IOSTREAM *in, ld_context *ctx)
{ int c;

  if ( !load_magic(in) )
    return FALSE;
  ctx->version = (int)load_int(in);
  if ( ctx->version < 2 || ctx->version > 3 )
  { term_t v = PL_new_term_ref();

    if ( PL_put_integer(v, ctx->version) )
      return PL_domain_error("rdf_db_save_version", v);
    else
      return FALSE;
  }

  while((c=Sgetc(in)) != EOF)
  { switch(c)
    { case 'T':
      { triple *t;

	if ( !(t=load_triple(db, in, ctx)) )
	  return FALSE;
	t->loaded = TRUE;
	buffer_triple(&ctx->triples, t);
        break;
      }
					/* file holding exactly one graph */
      case 'S':				/* name of the graph */
      { ctx->graph_name = load_atom(db, in, ctx);
        break;
      }
      case 'M':				/* MD5 of the graph */
      { int i;

	for(i=0; i<16; i++)
	  ctx->digest[i] = Sgetc(in);
	ctx->has_digest = TRUE;

	break;
      }
      case 'F':				/* file of the graph */
	ctx->graph_source = load_atom(db, in, ctx);
	break;				/* end of one-graph handling */
      case 't':
	load_double(in, &ctx->modified);
        break;
      case 'E':				/* end of file */
	return TRUE;
      default:
	break;
    }
  }

  return PL_warning("Illegal RDF triple file");
}


static int
prepare_loaded_triples(rdf_db *db, ld_context *ctx)
{ triple **t;

  if ( ctx->graph_name )			/* lookup named graph */
  { ctx->graph = lookup_graph(db, ctx->graph_name);
    if ( ctx->graph_source && ctx->graph->source != ctx->graph_source )
    { if ( ctx->graph->source )
	PL_unregister_atom(ctx->graph->source);
      ctx->graph->source = ctx->graph_source;
      PL_register_atom(ctx->graph->source);
      ctx->graph->modified = ctx->modified;
    }

    if ( ctx->has_digest )
    { if ( ctx->graph->md5 )
      { ctx->graph->md5 = FALSE;		/* kill repetitive MD5 update */
      } else
      { ctx->has_digest = FALSE;
      }
    }
  } else
  { ctx->graph = NULL;
  }

  for(t=ctx->triples.base; t<ctx->triples.top; t++)
    lock_atoms(db, *t);

  return TRUE;
}


static void
destroy_load_context(rdf_db *db, ld_context *ctx, int delete_triples)
{ if ( delete_triples )
  { triple **tp;

    for(tp=ctx->triples.base;
	tp<ctx->triples.top;
	tp++)
    { triple *t = *tp;

      free_triple(db, t, FALSE);
    }
  }

  free_triple_buffer(&ctx->triples);

  if ( ctx->atoms.loaded_objects )
  { atom_t *ap, *ep;

    for( ap=(atom_t*)ctx->atoms.loaded_objects, ep=ap+ctx->atoms.loaded_id;
	 ap<ep;
	 ap++)
    { PL_unregister_atom(*ap);
    }

    free(ctx->atoms.loaded_objects);
  }
  if ( ctx->predicates.loaded_objects )
    free(ctx->predicates.loaded_objects);
  if ( ctx->literals.loaded_objects )
    free(ctx->literals.loaded_objects);
}

typedef struct
{ term_t tail;
  term_t head;
} add_graph_context;

static int
append_graph_to_list(atom_t graph, void *closure)
{ add_graph_context *ctx = closure;

  return ( PL_unify_list(ctx->tail, ctx->head, ctx->tail) &&
	   PL_unify_atom(ctx->head, graph)
	 );
}


static foreign_t
rdf_load_db(term_t stream, term_t id, term_t graphs)
{ ld_context ctx;
  rdf_db *db = rdf_current_db();
  IOSTREAM *in;
  int rc;
  term_t ba_arg2;

  if ( !(ba_arg2 = PL_new_term_ref()) )
    return FALSE;

  if ( !PL_get_stream_handle(stream, &in) )
    return PL_type_error("stream", stream);

  memset(&ctx, 0, sizeof(ctx));
  init_atomset(&ctx.graph_table);
  init_triple_buffer(&ctx.triples);
  rc = load_db(db, in, &ctx);
  PL_release_stream(in);

  if ( !rc ||
       !PL_put_atom(ba_arg2, ATOM_begin) ||
       !rdf_broadcast(EV_LOAD, (void*)id, (void*)ba_arg2) )
  { destroy_load_context(db, &ctx, TRUE);
    return FALSE;
  }

  if ( (rc=prepare_loaded_triples(db, &ctx)) )
  { add_graph_context gctx;

    gctx.tail = PL_copy_term_ref(graphs);
    gctx.head = PL_new_term_ref();

    rc = ( for_atomset(&ctx.graph_table, append_graph_to_list, &gctx) &&
	   PL_unify_nil(gctx.tail) );

    destroy_atomset(&ctx.graph_table);
  }

  if ( rc )
  { query *q;

    if ( (q=open_query(db)) )
    { add_triples(q, ctx.triples.base, ctx.triples.top - ctx.triples.base);
      close_query(q);
    } else
    { goto error;
    }
    if ( ctx.graph )
    { if ( ctx.has_digest )
      { sum_digest(ctx.graph->digest, ctx.digest);
	ctx.graph->md5 = TRUE;
      }
      clear_modified(ctx.graph);
    }
    if ( (rc=PL_cons_functor(ba_arg2, FUNCTOR_end1, graphs)) )
      rc = rdf_broadcast(EV_LOAD, (void*)id, (void*)ba_arg2);
    destroy_load_context(db, &ctx, FALSE);

    return rc;
  }

error:
  rdf_broadcast(EV_LOAD, (void*)id, (void*)ATOM_error);
  destroy_load_context(db, &ctx, TRUE);
  return FALSE;
}


#ifdef WITH_MD5
		 /*******************************
		 *	     MD5 SUPPORT	*
		 *******************************/

/* md5_type is used to keep the MD5 independent from the internal
   numbers
*/
static const char md5_type[] =
{ 0x0,					/* OBJ_UNKNOWN */
  0x3,					/* OBJ_INTEGER */
  0x4,					/* OBJ_DOUBLE */
  0x2,					/* OBJ_STRING */
  0x5					/* OBJ_TERM */
};

static void
md5_triple(triple *t, md5_byte_t *digest)
{ md5_state_t state;
  size_t len;
  md5_byte_t tmp[2];
  const char *s;
  literal *lit;

  md5_init(&state);
  s = PL_blob_data(ID_ATOM(t->subject_id), &len, NULL);
  md5_append(&state, (const md5_byte_t *)s, (int)len);
  md5_append(&state, (const md5_byte_t *)"P", 1);
  s = PL_blob_data(t->predicate.r->name, &len, NULL);
  md5_append(&state, (const md5_byte_t *)s, (int)len);
  tmp[0] = 'O';
  if ( t->object_is_literal )
  { lit = t->object.literal;
    tmp[1] = md5_type[lit->objtype];

    switch(lit->objtype)
    { case OBJ_STRING:
	s = PL_blob_data(lit->value.string, &len, NULL);
	break;
      case OBJ_INTEGER:			/* TBD: byte order issues */
	s = (const char *)&lit->value.integer;
	len = sizeof(lit->value.integer);
	break;
      case OBJ_DOUBLE:
	s = (const char *)&lit->value.real;
	len = sizeof(lit->value.real);
	break;
      case OBJ_TERM:
	s = (const char *)lit->value.term.record;
	len = lit->value.term.len;
	break;
      default:
	assert(0);
    }
  } else
  { s = PL_blob_data(t->object.resource, &len, NULL);
    tmp[1] = 0x1;			/* old OBJ_RESOURCE */
    lit = NULL;
  }
  md5_append(&state, tmp, 2);
  md5_append(&state, (const md5_byte_t *)s, (int)len);
  if ( lit && lit->qualifier )
  { assert(lit->type_or_lang);
    md5_append(&state,
	       (const md5_byte_t *)(lit->qualifier == Q_LANG ? "l" : "t"),
	       1);
    s = PL_blob_data(ID_ATOM(lit->type_or_lang), &len, NULL);
    md5_append(&state, (const md5_byte_t *)s, (int)len);
  }
  if ( t->graph_id )
  { md5_append(&state, (const md5_byte_t *)"S", 1);
    s = PL_blob_data(ID_ATOM(t->graph_id), &len, NULL);
    md5_append(&state, (const md5_byte_t *)s, (int)len);
  }

  md5_finish(&state, digest);
}


static void
sum_digest(md5_byte_t *digest, md5_byte_t *add)
{ md5_byte_t *p, *q;
  int n;

  for(p=digest, q=add, n=16; --n>=0; )
    *p++ += *q++;
}


static void
dec_digest(md5_byte_t *digest, md5_byte_t *add)
{ md5_byte_t *p, *q;
  int n;

  for(p=digest, q=add, n=16; --n>=0; )
    *p++ -= *q++;
}


static int
md5_unify_digest(term_t t, md5_byte_t digest[16])
{ char hex_output[16*2];
  int di;
  char *pi;
  static char hexd[] = "0123456789abcdef";

  for(pi=hex_output, di = 0; di < 16; ++di)
  { *pi++ = hexd[(digest[di] >> 4) & 0x0f];
    *pi++ = hexd[digest[di] & 0x0f];
  }

  return PL_unify_atom_nchars(t, 16*2, hex_output);
}


static foreign_t
rdf_md5(term_t graph_name, term_t md5)
{ atom_t src;
  int rc;
  rdf_db *db = rdf_current_db();

  if ( !get_atom_or_var_ex(graph_name, &src) )
    return FALSE;

  if ( src )
  { graph *s;

    if ( (s = existing_graph(db, src)) && !s->erased )
    { rc = md5_unify_digest(md5, s->digest);
    } else
    { md5_byte_t digest[16];

      memset(digest, 0, sizeof(digest));
      rc = md5_unify_digest(md5, digest);
    }
  } else
  { md5_byte_t digest[16];
    int i;

    memset(&digest, 0, sizeof(digest));
    for(i=0; i<db->graphs.bucket_count; i++)
    { graph *g = db->graphs.blocks[MSB(i)][i];

      for( ; g; g = g->next )
	sum_digest(digest, g->digest);
    }

    return md5_unify_digest(md5, digest);
  }

  return rc;
}


static foreign_t
rdf_atom_md5(term_t text, term_t times, term_t md5)
{ char *s;
  int n, i;
  size_t len;
  md5_byte_t digest[16];

  if ( !PL_get_nchars(text, &len, &s, CVT_ALL|REP_UTF8|CVT_EXCEPTION) )
    return FALSE;
  if ( !PL_get_integer_ex(times, &n) )
    return FALSE;
  if ( n < 1 )
    return PL_domain_error("positive_integer", times);

  for(i=0; i<n; i++)
  { md5_state_t state;
    md5_init(&state);
    md5_append(&state, (const md5_byte_t *)s, (int)len);
    md5_finish(&state, digest);
    s = (char *)digest;
    len = sizeof(digest);
  }

  return md5_unify_digest(md5, digest);
}



#endif /*WITH_MD5*/


		 /*******************************
		 *	       ATOMS		*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Lock atoms in triple against AGC. Note that the predicate name is locked
in the predicate structure.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void
lock_atoms(rdf_db *db, triple *t)
{ if ( !t->atoms_locked )
  { t->atoms_locked = TRUE;

    register_resource(&db->resources, ID_ATOM(t->subject_id));
    if ( t->object_is_literal )
    { lock_atoms_literal(t->object.literal);
    } else
    { register_resource(&db->resources, t->object.resource);
    }
  }
}


static void
unlock_atoms(rdf_db *db, triple *t)
{ if ( t->atoms_locked )
  { t->atoms_locked = FALSE;

    unregister_resource(&db->resources, ID_ATOM(t->subject_id));
    if ( t->object_is_literal )
    { if ( !t->object.literal->shared )
	unlock_atoms_literal(t->object.literal);
    } else
    { unregister_resource(&db->resources, t->object.resource);
    }
  }
}


		 /*******************************
		 *      PROLOG CONVERSION	*
		 *******************************/

#define LIT_TYPED	0x1
#define LIT_NOERROR	0x2
#define LIT_PARTIAL	0x4

static int
get_lit_atom_ex(term_t t, atom_t *a, int flags)
{ if ( PL_get_atom(t, a) )
    return TRUE;
  if ( (flags & LIT_PARTIAL) && PL_is_variable(t) )
  { *a = 0L;
    return TRUE;
  }

  return PL_type_error("atom", t);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_literal() processes the argument  of  a   literal/1  term  passes as
object.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
get_literal(rdf_db *db, term_t litt, literal *lit, int flags)
{ if ( PL_get_atom(litt, &lit->value.string) )
  { lit->objtype = OBJ_STRING;
  } else if ( PL_is_integer(litt) && PL_get_int64(litt, &lit->value.integer) )
  { lit->objtype = OBJ_INTEGER;
  } else if ( PL_get_float(litt, &lit->value.real) )
  { lit->objtype = OBJ_DOUBLE;
  } else if ( PL_is_functor(litt, FUNCTOR_lang2) )
  { term_t a = PL_new_term_ref();
    atom_t tol;

    _PL_get_arg(1, litt, a);
    if ( !get_lit_atom_ex(a, &tol, flags) )
      return FALSE;
    lit->type_or_lang = ATOM_ID(tol);
    _PL_get_arg(2, litt, a);
    if ( !get_lit_atom_ex(a, &lit->value.string, flags) )
      return FALSE;

    lit->qualifier = Q_LANG;
    lit->objtype = OBJ_STRING;
  } else if ( PL_is_functor(litt, FUNCTOR_type2) &&
	      !(flags & LIT_TYPED) )	/* avoid recursion */
  { term_t a = PL_new_term_ref();
    atom_t tol;

    _PL_get_arg(1, litt, a);
    if ( !get_lit_atom_ex(a, &tol, flags) )
      return FALSE;
    lit->type_or_lang = ATOM_ID(tol);
    lit->qualifier = Q_TYPE;
    _PL_get_arg(2, litt, a);

    return get_literal(db, a, lit, LIT_TYPED|flags);
  } else if ( !PL_is_ground(litt) )
  { if ( !(flags & LIT_PARTIAL) )
      return PL_type_error("rdf_object", litt);
    if ( !PL_is_variable(litt) )
      lit->objtype = OBJ_TERM;
  } else
  { lit->value.term.record = PL_record_external(litt, &lit->value.term.len);
    lit->objtype = OBJ_TERM;
  }

  return TRUE;
}


static int
get_object(rdf_db *db, term_t object, triple *t)
{ if ( PL_get_atom(object, &t->object.resource) )
  { assert(!t->object_is_literal);
  } else if ( PL_is_functor(object, FUNCTOR_literal1) )
  { term_t a = PL_new_term_ref();

    _PL_get_arg(1, object, a);
    alloc_literal_triple(db, t);
    return get_literal(db, a, t->object.literal, 0);
  } else if ( get_prefixed_iri(db, object, &t->object.resource) )
  { assert(!t->object_is_literal);
  } else
    return PL_type_error("rdf_object", object);

  return TRUE;
}


static int
get_src(term_t src, triple *t)
{ if ( src && !PL_is_variable(src) )
  { atom_t at;

    if ( PL_get_atom(src, &at) )
    { t->graph_id = ATOM_ID(at);
      t->line = NO_LINE;
    } else if ( PL_is_functor(src, FUNCTOR_colon2) )
    { term_t a = PL_new_term_ref();
      long line;

      _PL_get_arg(1, src, a);
      if ( !get_atom_or_var_ex(a, &at) )
	return FALSE;
      t->graph_id = ATOM_ID(at);
      _PL_get_arg(2, src, a);
      if ( PL_get_long(a, &line) )
	t->line = line;
      else if ( !PL_is_variable(a) )
	return PL_type_error("integer", a);
    } else
      return PL_type_error("rdf_graph", src);
  }

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Return values:
	-1: exception
	 0: no predicate
	 1: the predicate
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
get_existing_predicate(rdf_db *db, term_t t, predicate **p)
{ atom_t name;

  if ( !PL_get_atom(t, &name ) )
  { if ( PL_is_functor(t, FUNCTOR_literal1) )
      return 0;				/* rdf(_, literal(_), _) */
    if ( get_prefixed_iri(db, t, &name) )
      goto ok;
    PL_type_error("rdf_predicate", t);
    return -1;
  }

ok:
  if ( (*p = existing_predicate(db, name)) )
    return 1;

  DEBUG(5, Sdprintf("No predicate %s\n", PL_atom_chars(name)));
  return 0;				/* no predicate */
}


static int
get_predicate(rdf_db *db, term_t t, predicate **p, query *q)
{ atom_t name;

  if ( !get_iri_ex(db, t, &name ) )
    return FALSE;

  *p = lookup_predicate(db, name);
  return TRUE;
}


static int
get_triple(rdf_db *db,
	   term_t subject, term_t predicate, term_t object,
	   triple *t, query *q)
{ atom_t at;

  if ( !get_iri_ex(db, subject, &at) ||
       !get_predicate(db, predicate, &t->predicate.r, q) ||
       !get_object(db, object, t) )
    return FALSE;

  t->subject_id = ATOM_ID(at);

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_partial_triple() creates a triple  for   matching  purposes.  It can
return FALSE for  two  reasons.  Mostly   (type)  errors,  but  also  if
resources are accessed that do not   exist  and therefore the subsequent
matching will always fail. This  is   notably  the  case for predicates,
which are first class citizens to this library.

Return values:
	1: ok
	0: no predicate
       -1: error
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
get_partial_triple(rdf_db *db,
		   term_t subject, term_t predicate, term_t object,
		   term_t src, triple *t)
{ int rc;
  int ipat = 0;

  if ( subject )
  { atom_t at;

    if ( !get_resource_or_var_ex(db, subject, &at) )
      return FALSE;
    t->subject_id = ATOM_ID(at);
  }
  if ( !PL_is_variable(predicate) &&
       (rc=get_existing_predicate(db, predicate, &t->predicate.r)) != 1 )
    return rc;
					/* the object */
  if ( object && !PL_is_variable(object) )
  { if ( PL_get_atom(object, &t->object.resource) )
    { assert(!t->object_is_literal);
    } else if ( PL_is_functor(object, FUNCTOR_literal1) )
    { term_t a = PL_new_term_ref();

      _PL_get_arg(1, object, a);
      alloc_literal_triple(db, t);
      if ( !get_literal(db, a, t->object.literal, LIT_PARTIAL) )
	return FALSE;
    } else if ( PL_is_functor(object, FUNCTOR_literal2) )
    { term_t a = PL_new_term_ref();
      literal *lit;

      alloc_literal_triple(db, t);
      lit = t->object.literal;

      _PL_get_arg(1, object, a);
      if ( PL_is_functor(a, FUNCTOR_exact1) )
	t->match = STR_MATCH_ICASE;
      else if ( PL_is_functor(a, FUNCTOR_icase1) )
	t->match = STR_MATCH_ICASE;
      else if ( PL_is_functor(a, FUNCTOR_plain1) )
	t->match = STR_MATCH_PLAIN;
      else if ( PL_is_functor(a, FUNCTOR_substring1) )
	t->match = STR_MATCH_SUBSTRING;
      else if ( PL_is_functor(a, FUNCTOR_word1) )
	t->match = STR_MATCH_WORD;
      else if ( PL_is_functor(a, FUNCTOR_prefix1) )
	t->match = STR_MATCH_PREFIX;
      else if ( PL_is_functor(a, FUNCTOR_like1) )
	t->match = STR_MATCH_LIKE;
      else if ( PL_is_functor(a, FUNCTOR_lt1) )
	t->match = STR_MATCH_LT;
      else if ( PL_is_functor(a, FUNCTOR_le1) )
	t->match = STR_MATCH_LE;
      else if ( PL_is_functor(a, FUNCTOR_eq1) )
	t->match = STR_MATCH_EQ;
      else if ( PL_is_functor(a, FUNCTOR_ge1) )
	t->match = STR_MATCH_GE;
      else if ( PL_is_functor(a, FUNCTOR_gt1) )
	t->match = STR_MATCH_GT;
      else if ( PL_is_functor(a, FUNCTOR_between2) )
      { term_t e = PL_new_term_ref();

	_PL_get_arg(2, a, e);
	memset(&t->tp.end, 0, sizeof(t->tp.end));
	if ( !get_literal(db, e, &t->tp.end, 0) )
	  return FALSE;
	t->match = STR_MATCH_BETWEEN;
      } else
	return PL_domain_error("match_type", a);

      _PL_get_arg(1, a, a);
      if ( t->match >= STR_MATCH_LT )
      { if ( !get_literal(db, a, lit, 0) )
	  return FALSE;
      } else
      { if ( !PL_get_atom_ex(a, &lit->value.string) )
	  return FALSE;
	lit->objtype = OBJ_STRING;
      }
    } else
      return PL_type_error("rdf_object", object);
  }
					/* the graph */
  if ( !get_src(src, t) )
    return FALSE;

  if ( t->subject_id )
    ipat |= BY_S;
  if ( t->predicate.r )
    ipat |= BY_P;
  if ( t->object_is_literal )
  { literal *lit = t->object.literal;

    switch( lit->objtype )
    { case OBJ_UNTYPED:
	break;
      case OBJ_STRING:
	if ( lit->objtype == OBJ_STRING )
	{ if ( lit->value.string &&
	       t->match <= STR_MATCH_ICASE )
	    ipat |= BY_O;
	}
        break;
      case OBJ_INTEGER:
      case OBJ_DOUBLE:
	ipat |= BY_O;
        break;
      case OBJ_TERM:
	if ( PL_is_ground(object) )
	  ipat |= BY_O;
        break;
      default:
	assert(0);
    }
  } else if ( t->object.resource )
  { ipat |= BY_O;
  }
  if ( t->graph_id )
    ipat |= BY_G;

  db->indexed[ipat]++;			/* statistics */
  t->indexed = alt_index[ipat];

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
inverse_partial_triple(triple *t) inverses a triple   by swapping object
and subject and replacing the predicate with its inverse.

TBD: In many cases we can  compute   the  hash  more efficiently than by
simply recomputing it:

  - Change predicate: x-or with old and new predicate hash
  - swap S<->O if the other is known is a no-op wrt the hash.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
inverse_partial_triple(triple *t)
{ predicate *i;

  if ( !t->inversed &&
       (!(i=t->predicate.r) || (i=t->predicate.r->inverse_of)) &&
       !t->object_is_literal )
  { atom_t o = t->object.resource;

    t->object.resource = t->subject_id ? ID_ATOM(t->subject_id) : 0;
    t->subject_id = o ? ATOM_ID(o) : 0;

    if ( t->predicate.r )
      t->predicate.r = i;

    t->indexed  = by_inverse[t->indexed];
    t->inversed = TRUE;

    return TRUE;
  }

  return FALSE;
}


static int
get_graph(term_t src, triple *t)
{ atom_t at;

  if ( PL_get_atom(src, &at) )
  { t->line = NO_LINE;
    t->graph_id = ATOM_ID(at);
    return TRUE;
  }

  if ( PL_is_functor(src, FUNCTOR_colon2) )
  { term_t a = PL_new_term_ref();
    long line;

    _PL_get_arg(1, src, a);
    if ( !PL_get_atom_ex(a, &at) )
      return FALSE;
    t->graph_id = ATOM_ID(at);
    _PL_get_arg(2, src, a);
    if ( !PL_get_long_ex(a, &line) )
      return FALSE;
    t->line = line;

    return TRUE;
  }

  return PL_type_error("rdf_graph", src);
}


static int
unify_graph(term_t src, triple *t)
{ switch( PL_term_type(src) )
  { case PL_VARIABLE:
    { if ( t->line == NO_LINE )
	return PL_unify_atom(src, ID_ATOM(t->graph_id));
      else
	goto full_term;
    }
    case PL_ATOM:
    { atom_t a;
      return (PL_get_atom(src, &a) &&
	      a == ID_ATOM(t->graph_id));
    }
    case PL_TERM:
    { if ( t->line == NO_LINE )
      { return PL_unify_term(src,
			     PL_FUNCTOR, FUNCTOR_colon2,
			       PL_ATOM, ID_ATOM(t->graph_id),
			       PL_VARIABLE);
      } else
      { full_term:
	return PL_unify_term(src,
			     PL_FUNCTOR, FUNCTOR_colon2,
			       PL_ATOM,  ID_ATOM(t->graph_id),
			       PL_INT64, (int64_t)t->line); /* line is uint32_t */
      }
    }
    default:
      return PL_type_error("rdf_graph", src);
  }
}


static int
same_graph(triple *t1, triple *t2)
{ return t1->line     == t2->line &&
         t1->graph_id == t2->graph_id;
}



static int
put_literal_value(term_t v, literal *lit)
{ switch(lit->objtype)
  { case OBJ_STRING:
      PL_put_atom(v, lit->value.string);
      break;
    case OBJ_INTEGER:
      PL_put_variable(v);
      return PL_unify_int64(v, lit->value.integer);
    case OBJ_DOUBLE:
      return PL_put_float(v, lit->value.real);
    case OBJ_TERM:
      return PL_recorded_external(lit->value.term.record, v);
    default:
      assert(0);
      return FALSE;
  }

  return TRUE;
}


static int
unify_literal(term_t lit, literal *l)
{ term_t v = PL_new_term_ref();

  if ( !put_literal_value(v, l) )
    return FALSE;

  if ( l->qualifier )
  { functor_t qf;

    assert(l->type_or_lang);

    if ( l->qualifier == Q_LANG )
      qf = FUNCTOR_lang2;
    else
      qf = FUNCTOR_type2;

    if ( PL_unify_term(lit, PL_FUNCTOR, qf,
			 PL_ATOM, ID_ATOM(l->type_or_lang),
			 PL_TERM, v) )
      return TRUE;

    if ( PL_exception(0) )
      return FALSE;

    return PL_unify(lit, v);		/* allow rdf(X, Y, literal(foo)) */
  } else if ( PL_unify(lit, v) )
  { return TRUE;
  } else if ( PL_is_functor(lit, FUNCTOR_lang2) &&
	      l->objtype == OBJ_STRING )
  { term_t a = PL_new_term_ref();
    _PL_get_arg(2, lit, a);
    return PL_unify(a, v);
  } else if ( PL_is_functor(lit, FUNCTOR_type2) )
  { term_t a = PL_new_term_ref();
    _PL_get_arg(2, lit, a);
    return PL_unify(a, v);
  } else
    return FALSE;
}



static int
unify_object(term_t object, triple *t)
{ if ( t->object_is_literal )
  { term_t lit = PL_new_term_ref();

    if ( PL_unify_functor(object, FUNCTOR_literal1) )
      _PL_get_arg(1, object, lit);
    else if ( PL_is_functor(object, FUNCTOR_literal2) )
      _PL_get_arg(2, object, lit);
    else
      return FALSE;

    return unify_literal(lit, t->object.literal);
  } else
  { return PL_unify_atom(object, t->object.resource);
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
TRUE:  ok
FALSE: failure
ERROR: error
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
unify_triple(term_t subject, term_t pred, term_t object,
	     term_t src, triple *t, int inversed)
{ predicate *p = t->predicate.r;
  fid_t fid = PL_open_foreign_frame();
  int rc;

  if ( inversed )
  { term_t tmp = object;
    object = subject;
    subject = tmp;

    rc = !pred || PL_unify_term(pred,
				PL_FUNCTOR, FUNCTOR_inverse_of1,
				  PL_ATOM, p->name);
  } else
  { rc = !pred || PL_unify_atom(pred, p->name);
  }

  if ( !rc ||
       !PL_unify_atom(subject, ID_ATOM(t->subject_id)) ||
       !unify_object(object, t) ||
       (src && !unify_graph(src, t)) )
  { if ( PL_exception(0) )
    { PL_close_foreign_frame(fid);
      return ERROR;
    }

    PL_discard_foreign_frame(fid);
    return FALSE;
  } else
  { PL_close_foreign_frame(fid);
    return TRUE;
  }
}


		 /*******************************
		 *	DUPLICATE HANDLING	*
		 *******************************/

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
According to the RDF specs, duplicate triples  have no meaning, but they
slow down search and often produce   duplicate results in search. Worse,
some coding styles proposed in the  OWL documents introduce huge amounts
of duplicate triples. We cannot  simply  ignore   a  triple  if  it is a
duplicate as a subsequent retract  would   delete  the final triple. For
example, after loading two  files  that   contain  the  same  triple and
unloading one of these files the database would be left without triples.

mark_duplicate() searches the DB for  a   duplicate  triple and sets the
flag is_duplicate on both. This flag is   used by rdf/3, where duplicate
triples are stored into a  temporary  table   to  be  filtered  from the
results by new_answer().

(*) We pick the write generation of the current query. This may still be
set higher, but that  that  may  only   lead  to  triples  being  marked
duplicates that are not. By use this   conservatie approach, we can move
mark_duplicate() into prelink_triple().

TBD: Duplicate marks may be removed by   GC:  walk over all triples that
are marked as duplicates and try to find the duplicate.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static void
mark_duplicate(rdf_db *db, triple *t, query *q)
{ triple_walker tw;
  triple *d;
  const int indexed = BY_SPO;
  lifespan qls;
  lifespan *ls;

  if ( q )
  { qls.born = queryWriteGen(q) + 1;		/* (*) */
    qls.died = query_max_gen(q);
    ls = &qls;
  } else
  { ls = &t->lifespan;
  }

  init_triple_walker(&tw, db, t, indexed);
  while((d=next_triple(&tw)) && d != t)
  { d = deref_triple(db, d);
    DEBUG(3, Sdprintf("Possible duplicate: ");
	     print_triple(d, PRT_NL|PRT_ADR));

    if ( !overlap_lifespan(&d->lifespan, ls) )
      continue;

    if ( match_triples(db, d, t, q, MATCH_DUPLICATE) )
    { if ( !t->is_duplicate )
      { t->is_duplicate = TRUE;
	db->duplicates++;
      }
      if ( !d->is_duplicate )
      { d->is_duplicate = TRUE;
	db->duplicates++;
      }
    }
  }
  destroy_triple_walker(db, &tw);
}


static int
update_duplicates(rdf_db *db)
{ triple *t;
  int count = 0;

  simpleMutexLock(&db->locks.duplicates);
  db->duplicates_up_to_date = FALSE;
  db->maintain_duplicates = FALSE;

  if ( db->duplicates )
  { enter_scan(&db->defer_all);
    for(t=fetch_triple(db, db->by_none.head);
	t;
	t=triple_follow_hash(db, t, ICOL(BY_NONE)))
    { if ( ++count % 10240 == 0 &&
	   (PL_handle_signals() < 0 || db->resetting) )

      { exit_scan(&db->defer_all);
	simpleMutexUnlock(&db->locks.duplicates);
	return FALSE;			/* aborted */
      }
      t->is_duplicate = FALSE;
    }
    exit_scan(&db->defer_all);

    db->duplicates = 0;
  }

  db->maintain_duplicates = TRUE;

  enter_scan(&db->defer_all);
  for(t=fetch_triple(db, db->by_none.head);
      t;
      t=triple_follow_hash(db, t, ICOL(BY_NONE)))
  { if ( ++count % 1024 == 0 &&
	 PL_handle_signals() < 0 )
    { exit_scan(&db->defer_all);
      db->maintain_duplicates = FALSE;		/* no point anymore */
      simpleMutexUnlock(&db->locks.duplicates);
      return FALSE;
    }
    mark_duplicate(db, t, NULL);
  }
  exit_scan(&db->defer_all);

  db->duplicates_up_to_date = TRUE;
  simpleMutexUnlock(&db->locks.duplicates);

  return TRUE;
}


static void
start_duplicate_admin(rdf_db *db)
{ db->maintain_duplicates = TRUE;

  PL_call_predicate(NULL, PL_Q_NORMAL,
		    PL_predicate("rdf_update_duplicates_thread", 0, "rdf_db"), 0);
}



		 /*******************************
		 *	    TRANSACTIONS	*
		 *******************************/

static int
put_begin_end(term_t t, functor_t be, int level)
{ term_t av;

  return ( (av = PL_new_term_ref()) &&
	   PL_put_integer(av, level) &&
	   PL_cons_functor_v(t, be, av) );
}


/** rdf_transaction(:Goal, +Id, +Options)

Options:

  * generation(+Generation)
  Determines query generation
*/

static int
transaction_depth(const query *q)
{ int depth = 0;

  for(q=q->transaction; q; q=q->transaction)
    depth++;

  return depth;
}


static foreign_t
rdf_transaction(term_t goal, term_t id, term_t options)
{ int rc;
  rdf_db *db = rdf_current_db();
  query *q;
  triple_buffer added;
  triple_buffer deleted;
  triple_buffer updated;
  snapshot *ss = NULL;

  if ( !PL_get_nil(options) )
  { term_t tail = PL_copy_term_ref(options);
    term_t head = PL_new_term_ref();
    term_t arg = PL_new_term_ref();

    while( PL_get_list(tail, head, tail) )
    { size_t arity;
      atom_t name;

      if ( !PL_get_name_arity(head, &name, &arity) || arity != 1 )
	return PL_type_error("option", head);
      _PL_get_arg(1, head, arg);

      if ( name == ATOM_snapshot )
      { if ( get_snapshot(arg, &ss) )
	{ int ss_tid = snapshot_thread(ss);

	  if ( ss_tid && ss_tid != PL_thread_self() )
	    PL_permission_error("access", "rdf-snapshot", arg);
	} else
	{ atom_t a;

	  if ( PL_get_atom(arg, &a) && a == ATOM_true )
	    ss = SNAPSHOT_ANONYMOUS;
	  else
	    return PL_type_error("rdf_snapshot", arg);
	}
      }
    }
    if ( !PL_get_nil_ex(tail) )
      return FALSE;
  }

  if ( !(q = open_transaction(db, &added, &deleted, &updated, ss)) )
    return FALSE;
  q->transaction_data.prolog_id = id;
  rc = PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, PRED_call1, goal);

  if ( rc )
  { if ( !empty_transaction(q) )
    { if ( ss )
      { discard_transaction(q);
      } else
      { term_t be;
	int depth = transaction_depth(q);

	if ( !(be=PL_new_term_ref()) ||
	     !put_begin_end(be, FUNCTOR_begin1, depth) ||
	     !rdf_broadcast(EV_TRANSACTION, (void*)id, (void*)be) ||
	     !put_begin_end(be, FUNCTOR_end1, depth) )
	  return FALSE;

	commit_transaction(q);

	if ( !rdf_broadcast(EV_TRANSACTION, (void*)id, (void*)be) )
	  return FALSE;
      }
    } else
    { close_transaction(q);
    }
  } else
  { discard_transaction(q);
  }

  return rc;
}

		 /*******************************
		 *	     PREDICATES		*
		 *******************************/

/** rdf_active_transactions_(-List)

Provides list of parent transactions in the calling thread
*/

static foreign_t
rdf_active_transactions(term_t list)
{ rdf_db *db = rdf_current_db();
  query *q = open_query(db);
  term_t tail = PL_copy_term_ref(list);
  term_t head = PL_new_term_ref();
  query *t;

  if ( !q ) return FALSE;
  for(t = q->transaction; t; t=t->transaction)
  { if ( !PL_unify_list(tail, head, tail) ||
         !PL_unify(head, t->transaction_data.prolog_id) )
    { close_query(q);
      return FALSE;
    }
  }

  close_query(q);

  return PL_unify_nil(tail);
}

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
(*) rdf_assert(S,P,O,G) adds a triple, but does not do so if exactly the
same quintuple is visible and not  yet   erased.  Adding  would not make
sense as this would be a complete duplicate that cannot be distinguished
from the original and rdf_retractall/4 will erase both.

Note that full duplicates are  quite  common   as  a  result  of forward
reasoning.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static foreign_t
rdf_assert4(term_t subject, term_t predicate, term_t object, term_t src)
{ rdf_db *db = rdf_current_db();
  query *q = open_query(db);
  triple *t, *d;
  triple_walker tw;

  if ( !q ) return FALSE;
  t = new_triple(db);
  if ( !get_triple(db, subject, predicate, object, t, q) )
  { error:
    free_triple(db, t, FALSE);
    close_query(q);
    return FALSE;
  }
  if ( src )
  { if ( !get_graph(src, t) )
      goto error;
  } else
  { t->graph_id = ATOM_ID(ATOM_user);
    t->line = NO_LINE;
  }

  init_triple_walker(&tw, db, t, BY_SPO);
  while((d=next_triple(&tw)))
  { if ( (d=alive_triple(q, d)) && !d->erased )		/* (*) */
    { if ( match_triples(db, d, t, q, MATCH_DUPLICATE|MATCH_SRC) &&
	   d->line == t->line )
      { destroy_triple_walker(db, &tw);
	free_triple(db, t, FALSE);
	close_query(q);

	return TRUE;
      }
    }
  }
  destroy_triple_walker(db, &tw);

  lock_atoms(db, t);

  add_triples(q, &t, 1);
  close_query(q);

  return TRUE;
}


static foreign_t
rdf_assert3(term_t subject, term_t predicate, term_t object)
{ return rdf_assert4(subject, predicate, object, 0);
}


static void	free_search_state(search_state *state);

static int
init_cursor_from_literal(search_state *state, literal *cursor)
{ triple *p = &state->pattern;
  size_t iv;

  DEBUG(3,
	Sdprintf("Trying literal search for ");
	print_literal(cursor);
	Sdprintf("\n"));

  p->indexed |= BY_O;
  p->indexed &= ~BY_G;			/* No graph indexing supported */
  if ( p->indexed == BY_SO )
  { p->indexed = BY_S;			/* we do not have index BY_SO */
    init_triple_walker(&state->cursor, state->db, p, p->indexed);
    return FALSE;
  }

  iv = literal_hash(cursor);		/* see also triple_hash_key() */
  if ( p->indexed&BY_S ) iv ^= subject_hash(p);
  if ( p->indexed&BY_P ) iv ^= predicate_hash(p->predicate.r);

  init_triple_literal_walker(&state->cursor, state->db, p, p->indexed, iv);
  state->has_literal_state = TRUE;
  state->literal_cursor = cursor;

  return TRUE;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
init_search_state(search_state *state, query *q)
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
init_search_state(search_state *state, query *query)
{ triple *p = &state->pattern;

  if ( get_partial_triple(state->db,
			  state->subject, state->predicate, state->object,
			  state->src, p) != TRUE )
  { free_search_state(state);
    return FALSE;
  }

  if ( p->object_is_literal && !is_numerical_string(p->object.literal) )
    state->flags &= ~MATCH_NUMERIC;

  if ( (p->match == STR_MATCH_PREFIX ||	p->match == STR_MATCH_LIKE) &&
       p->indexed != BY_SP &&
       (state->prefix = first_atom(p->object.literal->value.string, p->match)))
  { literal lit;
    literal **rlitp;

    lit = *p->object.literal;
    lit.value.string = state->prefix;
    state->lit_ex.literal = &lit;
    prepare_literal_ex(&state->lit_ex);
    rlitp = skiplist_find_first(&state->db->literals,
				&state->lit_ex, &state->literal_state);
    if ( rlitp )
    { if ( init_cursor_from_literal(state, *rlitp) )
      { state->restart_lit = *rlitp;
	state->restart_lit_state = state->literal_state;
      }
    } else
    { free_search_state(state);
      return FALSE;
    }
  } else if ( p->indexed != BY_SP && p->match >= STR_MATCH_LT )
  { literal **rlitp;

    state->lit_ex.literal = p->object.literal;
    prepare_literal_ex(&state->lit_ex);

    switch(p->match)
    { case STR_MATCH_LT:
      case STR_MATCH_LE:
	rlitp = skiplist_find_first(&state->db->literals,
				    NULL, &state->literal_state);
        break;
      case STR_MATCH_GT:
	rlitp = skiplist_find_first(&state->db->literals,
				    &state->lit_ex, &state->literal_state);
        break;
      case STR_MATCH_GE:
      case STR_MATCH_EQ:
	if ( (state->flags&MATCH_NUMERIC) ) /* xsd:double is lowest type */
	  p->object.literal->type_or_lang = ATOM_ID(ATOM_xsdDouble);
	rlitp = skiplist_find_first(&state->db->literals,
				    &state->lit_ex, &state->literal_state);
        break;
      case STR_MATCH_BETWEEN:
	if ( (state->flags&MATCH_NUMERIC) )
	  p->object.literal->type_or_lang = ATOM_ID(ATOM_xsdDouble);
	rlitp = skiplist_find_first(&state->db->literals,
				    &state->lit_ex, &state->literal_state);
        state->lit_ex.literal = &p->tp.end;
	prepare_literal_ex(&state->lit_ex);
        break;
      default:
	assert(0);
        return FALSE;
    }

    if ( rlitp )
    { if ( init_cursor_from_literal(state, *rlitp) )
      {	state->restart_lit = *rlitp;
	state->restart_lit_state = state->literal_state;
      }
    } else
    { free_search_state(state);
      return FALSE;
    }
  } else
  { init_triple_walker(&state->cursor, state->db, p, p->indexed);
  }

  return TRUE;
}


static void
free_search_state(search_state *state)
{ if ( state->query )
    close_query(state->query);

  free_triple(state->db, &state->pattern, FALSE);
  destroy_triple_walker(state->db, &state->cursor);
  if ( !state->db->maintain_duplicates &&
       state->dup_answers.count > state->db->duplicate_admin_threshold )
    start_duplicate_admin(state->db);
  destroy_tripleset(&state->dup_answers);

  if ( state->prefix )
    PL_unregister_atom(state->prefix);
}


static foreign_t
allow_retry_state(search_state *state)
{ PL_retry_address(state);
}


static int
new_answer(search_state *state, triple *t)
{ if ( !t->is_duplicate && state->db->duplicates_up_to_date )
    return TRUE;

  return add_tripleset(state, &state->dup_answers, t);
}


static triple *
is_candidate(search_state *state, triple *t)
{ if ( !(t=alive_triple(state->query, t)) )
    return NULL;
					/* hash-collision, skip */
  if ( state->has_literal_state )
  { if ( !(t->object_is_literal &&
	   t->object.literal == state->literal_cursor) )
      return NULL;
  }

  if ( !match_triples(state->db, t, &state->pattern, state->query, state->flags) )
    return NULL;

  if ( !state->src )				/* with source, we report */
  { if ( !new_answer(state, t) )		/* duplicates */
      return NULL;
  }

  return t;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
next_sub_property() advances the triple-walker to walk over an alternate
hash of the cloud.

  - If the cloud doesn't have ->alt_hashes, all related predicates have
    the same hash, and we are done.
  - If the cloud does have ->alt_hashes, we must walk the
    alt-hashes.  We do not need to walk hashes that do not use
    sub-properties of the target.  This is implemented using
    hash_holds_candidates().

TBD: How expensive is hash_holds_candidates(). Maybe  we should only try
that if there are many candidates  in the hash-chains? Alternatively, we
can keep a list of predicates that uses  a particular alt-hash, so we do
not have to scan the whole cloud each time.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
hash_holds_candidates(rdf_db *db, unsigned int hash,
		      predicate *p, predicate_cloud *pc,
		      query *q)
{ predicate **pp  = pc->members;
  predicate **end = &pp[pc->size];

  for(; pp<end; pp++)
  { predicate *p2 = *pp;

    if ( p2->hash == hash && isSubPropertyOf(db, p2, p, q) )
    { DEBUG(1, Sdprintf("\thash 0x%x: <%s rdfs:subPropertyOf %s>\n",
			hash, pname(p2), pname(p)));
      return TRUE;
    }
  }

  return FALSE;
}


static int
next_sub_property(search_state *state)
{ if ( (state->flags & MATCH_SUBPROPERTY) )
  { triple *p = &state->pattern;
    triple_walker *tw = &state->cursor;
    predicate_cloud *pc;

    if ( !(pc=state->p_cloud) )
    { if ( !p->predicate.r )		/* no pred on rdf_has(?,-,?) */
	return FALSE;

      if ( is_leaf_predicate(state->db, p->predicate.r, state->query) )
	return FALSE;

      if ( p->predicate.r->cloud->alt_hash_count )
      { pc = state->p_cloud = p->predicate.r->cloud;

	DEBUG(1, Sdprintf("%d alt hashes; first was 0x%x\n",
			  p->predicate.r->cloud->alt_hash_count,
			  predicate_hash(p->predicate.r)));
	tw->unbounded_hash ^= predicate_hash(p->predicate.r);
	state->alt_hash_cursor = 0;
      } else
	return FALSE;			/* Cloud has only one hash */
    } else
    { tw->unbounded_hash ^= pc->alt_hashes[state->alt_hash_cursor];
      state->alt_hash_cursor++;
    }

    for( ; state->alt_hash_cursor < pc->alt_hash_count; state->alt_hash_cursor++)
    { unsigned new_hash = pc->alt_hashes[state->alt_hash_cursor];

      if ( new_hash != predicate_hash(p->predicate.r) &&
	   hash_holds_candidates(state->db, new_hash,
				 p->predicate.r, pc, state->query) )
      { DEBUG(1, Sdprintf("Retrying with alt-hash %d (0x%x)\n",
			  state->alt_hash_cursor, new_hash));
	tw->unbounded_hash ^= new_hash;
	rewind_triple_walker(tw);

	return TRUE;
      }
    }
  }

  return FALSE;
}


/* next_pattern() advances the pattern for the next query.  This is done
   for matches that deal with matching inverse properties and matches
   that deal with literal ranges (prefix, between, etc.)

   Note that inverse and literal enumeration are mutually exclusive (as
   long as we do not have literal subjects ...).

   If we enumerate (sub)properties, we must enumerate the carthesian
   product of the sub properties and the inverse/literal search.
*/

static int
next_pattern(search_state *state)
{ triple_walker *tw = &state->cursor;
  triple *p = &state->pattern;

  if ( state->has_literal_state )
  { literal **litp;

    if ( (litp = skiplist_find_next(&state->literal_state)) )
    { literal *lit = *litp;

      DEBUG(2, Sdprintf("next: ");
	       print_literal(lit);
	       Sdprintf("\n"));

      switch(state->pattern.match)
      { case STR_MATCH_PREFIX:
	{ if ( !match_atoms(STR_MATCH_PREFIX, state->prefix, lit->value.string) )
	  { DEBUG(1,
		  Sdprintf("PREFIX: terminated literal iteration from ");
		  print_literal(lit);
		  Sdprintf("\n"));
	    return FALSE;			/* no longer a prefix */
	  }

	  break;
	}
	case STR_MATCH_LT:
	  if ( compare_literals(&state->lit_ex, lit) <= 0 )
	    return FALSE;
	case STR_MATCH_EQ:
	case STR_MATCH_LE:
	case STR_MATCH_BETWEEN:
	{ if ( (state->flags&MATCH_NUMERIC) )
	  { xsd_primary nt;

	    if ( (nt=is_numerical_string(lit)) )
	    { xsd_primary np = is_numerical_string(state->lit_ex.literal);

	      if ( cmp_xsd_info(np, &state->lit_ex.atom, nt, lit->value.string) < 0 )
		return FALSE;			/* no longer smaller/equal */

	      break;
	    }
	    return FALSE;
	  } else
	  { if ( compare_literals(&state->lit_ex, lit) < 0 )
	    { DEBUG(1,
		    Sdprintf("LE/BETWEEN(");
		    print_literal(state->lit_ex.literal);
		    Sdprintf("): terminated literal iteration from ");
		    print_literal(lit);
		    Sdprintf("\n"));
	      return FALSE;			/* no longer smaller/equal */
	    }
	  }

	  break;
	}
      }

      init_cursor_from_literal(state, lit);
      return TRUE;
    }
  }

  if ( next_sub_property(state) )	/* redo search with alternative hash */
  { if ( state->restart_lit )
    { state->literal_state = state->restart_lit_state;
      init_cursor_from_literal(state, state->restart_lit);
    }

    return TRUE;
  }

  if ( (state->flags&MATCH_INVERSE) &&
       inverse_partial_triple(p) )
  { DEBUG(1, Sdprintf("Retrying inverse: "); print_triple(p, PRT_NL));
    state->p_cloud = NULL;
    init_triple_walker(tw, state->db, p, p->indexed);

    return TRUE;
  }

  return FALSE;
}


static int
next_search_state(search_state *state)
{ triple *t, *t2;
  triple_walker *tw = &state->cursor;
  triple *p = &state->pattern;
  term_t retpred;

  if ( (state->flags & MATCH_SUBPROPERTY) )
  { retpred = state->realpred;
    if ( retpred )
    { if ( !p->predicate.r )		/* state->predicate is unbound */
      { if ( !PL_unify(state->predicate, retpred) )
	  return FALSE;
      }
    } else
    { if ( !p->predicate.r )
	retpred = state->predicate;
    }
  } else
  { retpred = p->predicate.r ? 0 : state->predicate;
  }

  if ( (t2=state->prefetched) )
  { state->prefetched = NULL;		/* retrying; to need to check */
    goto retry;
  }

  do
  { while( (t = next_triple(tw)) )
    { DEBUG(3, Sdprintf("Search: ");
	       print_triple(t, PRT_SRC|PRT_GEN|PRT_NL|PRT_ADR));

      if ( (t2=is_candidate(state, t)) )
      { int rc;

      retry:
	if ( (rc=unify_triple(state->subject, retpred, state->object,
			      state->src, t2, p->inversed)) == FALSE )
	  continue;
	if ( rc == ERROR )
	  return FALSE;			/* makes rdf/3 return FALSE */

	do
	{ while( (t = next_triple(tw)) )
	  { DEBUG(3, Sdprintf("Search (prefetch): ");
		  print_triple(t, PRT_SRC|PRT_GEN|PRT_NL|PRT_ADR));

	    if ( (t2=is_candidate(state, t)) )
	    { state->prefetched = t2;

	      return TRUE;		/* non-deterministic */
	    }
	  }
	} while(next_pattern(state));

	return TRUE;			/* deterministic */
      }
    }
  } while(next_pattern(state));

  return FALSE;
}



static foreign_t
rdf(term_t subject, term_t predicate, term_t object,
    term_t src, term_t realpred, control_t h, unsigned flags)
{ rdf_db *db = rdf_current_db();
  search_state *state;

  switch(PL_foreign_control(h))
  { case PL_FIRST_CALL:
    { query *q = open_query(db);

      if ( !q ) return FALSE;

      state = &q->state.search;
      state->query     = q;
      state->db	       = db;
      state->subject   = subject;
      state->object    = object;
      state->predicate = predicate;
      state->src       = src;
      state->realpred  = realpred;
      state->flags     = flags;
						/* clear the rest */
      memset(&state->cursor, 0,
	     (char*)&state->lit_ex - (char*)&state->cursor);
      state->dup_answers.entries = NULL;	/* see add_tripleset() */

      if ( !init_search_state(state, q) )
	return FALSE;

      goto search;
    }
    case PL_REDO:
    { int rc;

      state = PL_foreign_context_address(h);
      assert(state->subject == subject);

    search:
      if ( (rc=next_search_state(state)) )
      { if ( state->prefetched )
	  return allow_retry_state(state);
      }

      free_search_state(state);
      return rc;
    }
    case PL_PRUNED:
    { state = PL_foreign_context_address(h);

      free_search_state(state);
      return TRUE;
    }
    default:
      assert(0);
      return FALSE;
  }
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
rdf(Subject, Predicate, Object)

Search specifications:

	Predicate:

		subPropertyOf(X) = P

	Object:

		literal(substring(X), L)
		literal(word(X), L)
		literal(exact(X), L)
		literal(icase(X), L)
		literal(prefix(X), L)
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */


static foreign_t
rdf3(term_t subject, term_t predicate, term_t object, control_t h)
{ return rdf(subject, predicate, object, 0, 0, h,
	     MATCH_EXACT|MATCH_NUMERIC);
}

static foreign_t
rdf4(term_t subject, term_t predicate, term_t object,
     term_t src, control_t h)
{ return rdf(subject, predicate, object, src, 0, h,
	     MATCH_EXACT|MATCH_NUMERIC|MATCH_SRC);
}


static foreign_t
rdf_has3(term_t subject, term_t predicate, term_t object, control_t h)
{ return rdf(subject, predicate, object, 0, 0, h,
	     MATCH_EXACT|MATCH_NUMERIC|MATCH_SUBPROPERTY|MATCH_INVERSE);
}


static foreign_t
rdf_has4(term_t subject, term_t predicate, term_t object,
	term_t realpred, control_t h)
{ return rdf(subject, predicate, object, 0, realpred, h,
	     MATCH_EXACT|MATCH_NUMERIC|MATCH_SUBPROPERTY|MATCH_INVERSE);
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
rdf_estimate_complexity(+S,+P,+O,-C)

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static foreign_t
rdf_estimate_complexity(term_t subject, term_t predicate, term_t object,
		        term_t complexity)
{ triple t;
  size_t c;
  rdf_db *db = rdf_current_db();
  int rc;

  memset(&t, 0, sizeof(t));
  if ( (rc=get_partial_triple(db, subject, predicate, object, 0, &t)) != TRUE )
  { if ( rc == -1 )
    { return FALSE;			/* error */
    } else
    { return PL_unify_integer(complexity, 0);	/* no predicate */
    }
  }

  if ( t.indexed == BY_NONE )
  { c = db->created - db->erased;		/* = totale triple count */
#if 0
  } else if ( t.indexed == BY_P )
  { c = t.predicate.r->triple_count;		/* must sum over children */
#endif
  } else
  { size_t key = triple_hash_key(&t, t.indexed);
    int icol = ICOL(t.indexed);
    triple_hash *hash = &db->hash[icol];
    size_t count;

    if ( !db->hash[icol].created )
      create_triple_hashes(db, 1, &icol);

    c = 0;
    for(count=hash->bucket_count_epoch; count <= hash->bucket_count; count *= 2)
    { int entry = key%count;
      triple_bucket *bucket = &hash->blocks[MSB(entry)][entry];

      c += bucket->count;		/* TBD: compensate for resize */
    }
  }

  rc = PL_unify_int64(complexity, c);
  free_triple(db, &t, FALSE);

  return rc;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
current_literal(?Literals)
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

typedef struct cl_state
{ skiplist_enum sl_state;
  int		indexed;
  literal	lit;
  literal_ex	lit_ex;
} cl_state;

static int
indexedLiteral(const literal *lit)
{ if ( lit->objtype == OBJ_STRING )
    return lit->value.string != 0;
  return lit->objtype != OBJ_UNTYPED;
}


static foreign_t
rdf_current_literal(term_t t, control_t h)
{ rdf_db *db = rdf_current_db();
  literal **data;
  cl_state *state;
  int rc;

  switch(PL_foreign_control(h))
  { case PL_FIRST_CALL:
      state = rdf_malloc(db, sizeof(*state));
      memset(state, 0, sizeof(*state));

      if ( PL_is_variable(t) )
      { data = skiplist_find_first(&db->literals, NULL, &state->sl_state);
	goto next;
      } else
      { if ( !get_literal(db, t, &state->lit, LIT_PARTIAL) )
	{ rdf_free(db, state, sizeof(*state));
	  return FALSE;
	}
	if ( indexedLiteral(&state->lit) )
	{ state->lit_ex.literal = &state->lit;
	  prepare_literal_ex(&state->lit_ex);
	  data = skiplist_find_first(&db->literals,
				     &state->lit_ex, &state->sl_state);
	  state->indexed = TRUE;
	} else
	{ data = skiplist_find_first(&db->literals, NULL, &state->sl_state);
	}
	goto next;
      }
    case PL_REDO:
      state = PL_foreign_context_address(h);
      data  = skiplist_find_next(&state->sl_state);
    next:
    { fid_t fid = PL_open_foreign_frame();

      for(; data; data=skiplist_find_next(&state->sl_state))
      { literal *lit = *data;

	if ( unify_literal(t, lit) )
	{ PL_close_foreign_frame(fid);
	  PL_retry_address(state);
	} else if ( PL_exception(0) )
	{ break;
	} else if ( state->indexed &&
		    compare_literals(&state->lit_ex, lit) > 0 )
	{ break;
	} else
	{ PL_rewind_foreign_frame(fid);
	}
      }
      PL_close_foreign_frame(fid);
      rc = FALSE;
      goto cleanup;
    }
    case PL_PRUNED:
      state = PL_foreign_context_address(h);
      rc = TRUE;

    cleanup:
      free_literal(db, &state->lit);
      rdf_free(db, state, sizeof(*state));

      return rc;
    default:
      assert(0);
      return FALSE;
  }
}


static int
update_triple(rdf_db *db, term_t action, triple *t, triple **updated, query *q)
{ term_t a = PL_new_term_ref();
  triple tmp, *new;
					/* Create copy in local memory */
  tmp = *t;

  if ( !PL_get_arg(1, action, a) )
    return PL_type_error("rdf_action", action);

  if ( PL_is_functor(action, FUNCTOR_subject1) )
  { atom_t s;

    if ( !PL_get_atom_ex(a, &s) )
      return FALSE;
    if ( tmp.subject_id == ATOM_ID(s) )
      return TRUE;			/* no change */

    tmp.subject_id = ATOM_ID(s);
  } else if ( PL_is_functor(action, FUNCTOR_predicate1) )
  { predicate *p;

    if ( !get_predicate(db, a, &p, q) )
      return FALSE;
    if ( tmp.predicate.r == p )
      return TRUE;			/* no change */

    tmp.predicate.r = p;
  } else if ( PL_is_functor(action, FUNCTOR_object1) )
  { triple t2;

    memset(&t2, 0, sizeof(t2));

    if ( !get_object(db, a, &t2) )
    { free_triple(db, &t2, FALSE);
      return FALSE;
    }
    if ( match_object(&t2, &tmp, MATCH_QUAL) )
    { free_triple(db, &t2, FALSE);
      return TRUE;
    }

    if ( (tmp.object_is_literal = t2.object_is_literal) )
    { tmp.object.literal = t2.object.literal;
    } else
    { tmp.object.resource = t2.object.resource;
    }
  } else if ( PL_is_functor(action, FUNCTOR_graph1) )
  { triple t2;

    if ( !get_graph(a, &t2) )
      return FALSE;
    if ( t2.graph_id == t->graph_id && t2.line == t->line )
    { *updated = NULL;
      return TRUE;
    }

    tmp.graph_id = t2.graph_id;
    tmp.line = t2.line;
  } else
    return PL_domain_error("rdf_action", action);

  new = new_triple(db);
  new->subject_id	 = tmp.subject_id;
  new->predicate.r	 = tmp.predicate.r;
  if ( (new->object_is_literal = tmp.object_is_literal) )
  { if ( tmp.object.literal->shared )
    { simpleMutexLock(&db->locks.literal);
      new->object.literal = copy_literal(db, tmp.object.literal);
      simpleMutexUnlock(&db->locks.literal);
    } else
    { new->object.literal = tmp.object.literal;
    }
  } else
  { new->object.resource = tmp.object.resource;
  }
  new->graph_id		 = tmp.graph_id;
  new->line		 = tmp.line;

  lock_atoms(db, new);

  *updated = new;

  return TRUE;
}


/** rdf_update(+Subject, +Predicate, +Object, +Action) is det.

Update a triple. Please note this is actually erase+assert
*/

static foreign_t
rdf_update5(term_t subject, term_t predicate, term_t object, term_t src,
	    term_t action)
{ triple t, *p;
  int indexed = BY_SPO;
  rdf_db *db = rdf_current_db();
  int rc = TRUE;
  size_t count;
  triple_walker tw;
  triple_buffer matches;
  query *q = open_query(db);

  if ( !q ) return FALSE;
  memset(&t, 0, sizeof(t));

  if ( !get_src(src, &t) ||
       !get_triple(db, subject, predicate, object, &t, q) )
  { close_query(q);
    return FALSE;
  }

  init_triple_buffer(&matches);
  init_triple_walker(&tw, db, &t, indexed);
  while((p=next_triple(&tw)))
  { if ( !(p=alive_triple(q, p)) )
      continue;

    if ( match_triples(db, p, &t, q, MATCH_EXACT) )
      buffer_triple(&matches, p);
  }

  if ( !is_empty_buffer(&matches) )
  { triple_buffer replacements;
    triple *new, **tp;

    count = matches.top-matches.base;
    init_triple_buffer(&replacements);
    for(tp=matches.base; tp<matches.top; tp++)
    { new = NULL;
      if ( !update_triple(db, action, *tp, &new, q) )
      { rc = FALSE;
	free_triple_buffer(&replacements);
	goto out;
      }

      buffer_triple(&replacements, new);
    }

    update_triples(q, matches.base, replacements.base, count);
    free_triple_buffer(&replacements);
  } else
  { count = 0;
  }

out:
  close_query(q);
  free_triple_buffer(&matches);
  free_triple(db, &t, FALSE);

  return (rc && count > 0) ? TRUE : FALSE;
}


static foreign_t
rdf_update(term_t subject, term_t predicate, term_t object, term_t action)
{ return rdf_update5(subject, predicate, object, 0, action);
}


static foreign_t
rdf_retractall4(term_t subject, term_t predicate, term_t object, term_t src)
{ triple t, *p;
  rdf_db *db = rdf_current_db();
  triple_walker tw;
  triple_buffer buf;
  query *q;

  memset(&t, 0, sizeof(t));
  switch( get_partial_triple(db, subject, predicate, object, src, &t) )
  { case 0:				/* no such predicate */
      return TRUE;
    case -1:				/* error */
      return FALSE;
  }

  if ( t.graph_id )		/* speedup for rdf_retractall(_,_,_,DB) */
  { graph *gr = existing_graph(db, ID_ATOM(t.graph_id));

    if ( !gr || gr->triple_count == 0 )
      return TRUE;
  }

  if ( !(q = open_query(db)) )
    return FALSE;
  init_triple_buffer(&buf);
  init_triple_walker(&tw, db, &t, t.indexed);
  while((p=next_triple(&tw)))
  { if ( !(p=alive_triple(q, p)) )
      continue;

    if ( match_triples(db, p, &t, q, MATCH_EXACT|MATCH_SRC) )
    { if ( t.object_is_literal && t.object.literal->objtype == OBJ_TERM )
      { fid_t fid = PL_open_foreign_frame();
	int rc = unify_object(object, p);
	PL_discard_foreign_frame(fid);
	if ( !rc )
	  continue;
      }

      buffer_triple(&buf, p);
    }
  }
  free_triple(db, &t, FALSE);
  del_triples(q, buf.base, buf.top-buf.base);
  close_query(q);
  free_triple_buffer(&buf);


  return TRUE;
}


static foreign_t
rdf_retractall3(term_t subject, term_t predicate, term_t object)
{ return rdf_retractall4(subject, predicate, object, 0);
}


		 /*******************************
		 *	     MONITOR		*
		 *******************************/

typedef struct broadcast_callback
{ struct broadcast_callback *next;
  predicate_t		     pred;
  long			     mask;
} broadcast_callback;

static long joined_mask = 0L;
static broadcast_callback *callback_list;
static broadcast_callback *callback_tail;

static int
do_broadcast(term_t term, long mask)
{ if ( callback_list )
  { broadcast_callback *cb;

    for(cb = callback_list; cb; cb = cb->next)
    { qid_t qid;
      term_t ex;

      if ( !(cb->mask & mask) )
	continue;

      if ( !(qid = PL_open_query(NULL, PL_Q_CATCH_EXCEPTION, cb->pred, term)) )
	return FALSE;
      if ( !PL_next_solution(qid) && (ex = PL_exception(qid)) )
      { term_t av;

	PL_cut_query(qid);

	if ( (av = PL_new_term_refs(2)) &&
	     PL_put_atom(av+0, ATOM_error) &&
	     PL_put_term(av+1, ex) )
	  PL_call_predicate(NULL, PL_Q_NORMAL,
			    PL_predicate("print_message", 2, "user"),
			    av);
	return FALSE;
      } else
      { PL_close_query(qid);
      }
    }
  }

  return TRUE;
}


int
rdf_is_broadcasting(broadcast_id id)
{ return (joined_mask & id) != 0;
}


int
rdf_broadcast(broadcast_id id, void *a1, void *a2)
{ int rc = TRUE;

  if ( (joined_mask & id) )
  { fid_t fid;
    term_t term;
    functor_t funct;

    if ( !(fid = PL_open_foreign_frame()) ||
	 !(term = PL_new_term_ref()) )
      return FALSE;

    switch(id)
    { case EV_ASSERT:
      case EV_ASSERT_LOAD:
	funct = FUNCTOR_assert4;
        goto assert_retract;
      case EV_RETRACT:
	funct = FUNCTOR_retract4;
      assert_retract:
      { triple *t = a1;
	term_t tmp;

	if ( !(tmp = PL_new_term_refs(4)) ||
	     !PL_put_atom(tmp+0, ID_ATOM(t->subject_id)) ||
	     !PL_put_atom(tmp+1, t->predicate.r->name) ||
	     !unify_object(tmp+2, t) ||
	     !unify_graph(tmp+3, t) ||
	     !PL_cons_functor_v(term, funct, tmp) )
	  return FALSE;
	break;
      }
      case EV_UPDATE:
      { triple *t = a1;
	triple *new = a2;
	term_t tmp, a;
	functor_t action;
	int rc;

	if ( !(tmp = PL_new_term_refs(5)) ||
	     !(a = PL_new_term_ref()) ||
	     !PL_put_atom(tmp+0, ID_ATOM(t->subject_id)) ||
	     !PL_put_atom(tmp+1, t->predicate.r->name) ||
	     !unify_object(tmp+2, t) ||
	     !unify_graph(tmp+3, t) )
	  return FALSE;

	if ( t->subject_id != new->subject_id )
	{ action = FUNCTOR_subject1;
	  rc = PL_put_atom(a, ID_ATOM(new->subject_id));
	} else if ( t->predicate.r != new->predicate.r )
	{ action = FUNCTOR_predicate1;
	  rc = PL_put_atom(a, new->predicate.r->name);
	} else if ( !match_object(t, new, MATCH_QUAL) )
	{ action = FUNCTOR_object1;
	  rc = unify_object(a, new);
	} else if ( !same_graph(t, new) )
	{ action = FUNCTOR_graph1;
	  rc = unify_graph(a, new);
	} else
	{ return TRUE;			/* no change */
	}

        if ( !rc ||
	     !PL_cons_functor_v(tmp+4, action, a) ||
	     !PL_cons_functor_v(term, FUNCTOR_update5, tmp) )
	  return FALSE;
	break;
      }
      case EV_NEW_LITERAL:
      { literal *lit = a1;
	term_t tmp;

	if ( !(tmp = PL_new_term_refs(1)) ||
	     !unify_literal(tmp, lit) ||
	     !PL_cons_functor_v(term, FUNCTOR_new_literal1, tmp) )
	  return FALSE;
	break;
      }
      case EV_OLD_LITERAL:
      { literal *lit = a1;
	term_t tmp;

	if ( !(tmp = PL_new_term_refs(1)) ||
	     !unify_literal(tmp, lit) ||
	     !PL_cons_functor_v(term, FUNCTOR_old_literal1, tmp) )
	  return FALSE;
	break;
      }
      case EV_LOAD:
      { term_t ctx = (term_t)a1;
	term_t be  = (term_t)a2;
	term_t tmp;

	if ( !(tmp = PL_new_term_refs(2)) ||
	     !PL_put_term(tmp+0, be) ||		/* begin/end(graphs) */
	     !PL_put_term(tmp+1, ctx) ||
	     !PL_cons_functor_v(term, FUNCTOR_load2, tmp) )
	  return FALSE;
	break;
      }
      case EV_TRANSACTION:
      { term_t ctx = (term_t)a1;
	term_t be  = (term_t)a2;
	term_t tmp;

	if ( !(tmp = PL_new_term_refs(2)) ||
	     !PL_put_term(tmp+0, be) ||		/* begin/end */
	     !PL_put_term(tmp+1, ctx) ||
	     !PL_cons_functor_v(term, FUNCTOR_transaction2, tmp) )
	  return FALSE;
	break;
      }
      case EV_RESET:
      { PL_put_atom(term, ATOM_reset);
	break;
      }
      case EV_CREATE_GRAPH:
      { graph *g = a1;
	term_t tmp;

	if ( !(tmp = PL_new_term_refs(1)) ||
	     !(PL_put_atom(tmp, g->name)) ||
	     !PL_cons_functor_v(term, FUNCTOR_create_graph1, tmp) )
	  return FALSE;
	break;
      }
      default:
	assert(0);
    }

    rc = do_broadcast(term, id);

    PL_discard_foreign_frame(fid);
  }

  return rc;
}


static foreign_t
rdf_monitor(term_t goal, term_t mask)
{ atom_t name;
  broadcast_callback *cb;
  predicate_t p;
  long msk;
  module_t m = NULL;

  if ( !PL_strip_module(goal, &m, goal) ||
       !PL_get_atom_ex(goal, &name) ||
       !PL_get_long_ex(mask, &msk) )
    return FALSE;

  p = PL_pred(PL_new_functor(name, 1), m);

  for(cb=callback_list; cb; cb = cb->next)
  { if ( cb->pred == p )
    { broadcast_callback *cb2;
      cb->mask = msk;

      joined_mask = 0L;
      for(cb2=callback_list; cb2; cb2 = cb2->next)
	joined_mask |= cb2->mask;
      DEBUG(2, Sdprintf("Set mask to 0x%x\n", joined_mask));

      return TRUE;
    }
  }

  cb = PL_malloc(sizeof(*cb));
  cb->next = NULL;
  cb->mask = msk;
  cb->pred = p;
  if ( callback_list )
  { callback_tail->next = cb;
    callback_tail = cb;
  } else
  { callback_list = callback_tail = cb;
  }
  joined_mask |= msk;

  return TRUE;
}



static foreign_t
rdf_set_predicate(term_t pred, term_t option)
{ predicate *p;
  rdf_db *db = rdf_current_db();
  query *q = open_query(db);
  int rc;

  if ( !q ) return FALSE;
  if ( !get_predicate(db, pred, &p, q) )
  { rc = FALSE;
    goto out;
  }

  if ( PL_is_functor(option, FUNCTOR_symmetric1) )
  { int val;

    if ( !get_bool_arg_ex(1, option, &val) )
    { rc = FALSE;
      goto out;
    }

    if ( val )
      p->inverse_of = p;
    else
      p->inverse_of = NULL;

    rc = TRUE;
  } else if ( PL_is_functor(option, FUNCTOR_inverse_of1) )
  { term_t a = PL_new_term_ref();
    predicate *i;

    _PL_get_arg(1, option, a);
    if ( PL_get_nil(a) )
    { if ( p->inverse_of )
      { p->inverse_of->inverse_of = NULL;
	p->inverse_of = NULL;
      }
    } else
    { if ( !get_predicate(db, a, &i, q) )
      { rc = FALSE;
	goto out;
      }

      p->inverse_of = i;
      i->inverse_of = p;
    }
    rc = TRUE;
  } else if ( PL_is_functor(option, FUNCTOR_transitive1) )
  { int val;

    if ( !get_bool_arg_ex(1, option, &val) )
      return FALSE;

    p->transitive = val;

    rc = TRUE;
  } else
    rc = PL_type_error("predicate_option", option);

out:
  close_query(q);
  return rc;
}


#define PRED_PROPERTY_COUNT 9
static functor_t predicate_key[PRED_PROPERTY_COUNT];

static int
unify_predicate_property(rdf_db *db, predicate *p, term_t option,
			 functor_t f, query *q)
{ if ( f == FUNCTOR_symmetric1 )
    return PL_unify_term(option, PL_FUNCTOR, f,
			 PL_BOOL, p->inverse_of == p ? TRUE : FALSE);
  else if ( f == FUNCTOR_inverse_of1 )
  { if ( p->inverse_of )
      return PL_unify_term(option, PL_FUNCTOR, f,
			   PL_ATOM, p->inverse_of->name);
    else
      return FALSE;
  } else if ( f == FUNCTOR_transitive1 )
  { return PL_unify_term(option, PL_FUNCTOR, f,
			 PL_BOOL, p->transitive);
  } else if ( f == FUNCTOR_triples1 )
  { return PL_unify_term(option, PL_FUNCTOR, f,
			 PL_LONG, p->triple_count);
  } else if ( f == FUNCTOR_rdf_subject_branch_factor1 )
  { return PL_unify_term(option, PL_FUNCTOR, f,
		 PL_FLOAT, subject_branch_factor(db, p, q, DISTINCT_DIRECT));
  } else if ( f == FUNCTOR_rdf_object_branch_factor1 )
  { return PL_unify_term(option, PL_FUNCTOR, f,
		 PL_FLOAT, object_branch_factor(db, p, q, DISTINCT_DIRECT));
  } else if ( f == FUNCTOR_rdfs_subject_branch_factor1 )
  { return PL_unify_term(option, PL_FUNCTOR, f,
		 PL_FLOAT, subject_branch_factor(db, p, q, DISTINCT_SUB));
  } else if ( f == FUNCTOR_rdfs_object_branch_factor1 )
  { return PL_unify_term(option, PL_FUNCTOR, f,
		 PL_FLOAT, object_branch_factor(db, p, q, DISTINCT_SUB));
  } else
  { assert(0);
    return FALSE;
  }
}


typedef struct enum_pred
{ predicate *p;
  int i;
} enum_pred;


static foreign_t
rdf_current_predicate(term_t name, control_t h)
{ rdf_db *db = rdf_current_db();
  predicate *p;
  enum_pred *ep;
  atom_t a;

  switch( PL_foreign_control(h) )
  { case PL_FIRST_CALL:
      if ( PL_is_variable(name) )
      { ep = rdf_malloc(db, sizeof(*ep));
	ep->i  = 0;
	ep->p  = NULL;
	goto next;
      } else if ( PL_get_atom(name, &a) )
      { return existing_predicate(db, a) != NULL;
      } else if ( PL_is_functor(name, FUNCTOR_literal1) )
      { return FALSE;
      }

      return PL_type_error("atom", name);
    case PL_REDO:
      ep = PL_foreign_context_address(h);
      goto next;
    case PL_PRUNED:
      ep = PL_foreign_context_address(h);
      rdf_free(db, ep, sizeof(*ep));
      return TRUE;
    default:
      assert(0);
      return FALSE;
  }

next:
  if ( !(p=ep->p) )
  { while (!(p = db->predicates.blocks[MSB(ep->i)][ep->i]) )
    { if ( ++ep->i >= db->predicates.bucket_count )
	goto fail;
    }
  }

  if ( !PL_unify_atom(name, p->name) )
  { fail:
    rdf_free(db, ep, sizeof(*ep));
    return FALSE;
  }

  if ( !(ep->p = p->next) )
  { if ( ++ep->i >= db->predicates.bucket_count )
    { rdf_free(db, ep, sizeof(*ep));
      return TRUE;
    }
  }
  PL_retry_address(ep);
}


static foreign_t
rdf_predicate_property(term_t pred, term_t option, control_t h)
{ predicate *p;
  rdf_db *db = rdf_current_db();
  query *q;

  if ( !predicate_key[0] )
  { int i = 0;

    predicate_key[i++] = FUNCTOR_symmetric1;
    predicate_key[i++] = FUNCTOR_inverse_of1;
    predicate_key[i++] = FUNCTOR_transitive1;
    predicate_key[i++] = FUNCTOR_triples1;
    predicate_key[i++] = FUNCTOR_rdf_subject_branch_factor1;
    predicate_key[i++] = FUNCTOR_rdf_object_branch_factor1;
    predicate_key[i++] = FUNCTOR_rdfs_subject_branch_factor1;
    predicate_key[i++] = FUNCTOR_rdfs_object_branch_factor1;
    assert(i < PRED_PROPERTY_COUNT);
  }

  switch(PL_foreign_control(h))
  { case PL_FIRST_CALL:
    { functor_t f;
      int rc;

      if ( !(q = open_query(db)) )
	return FALSE;
      if ( PL_is_variable(option) )
      { q->state.predprop.prop = 0;
	if ( !get_predicate(db, pred, &q->state.predprop.pred, q) )
	{ close_query(q);
	  return FALSE;
	}
	goto redo;
      } else if ( PL_get_functor(option, &f) )
      { int n;

	for(n=0; predicate_key[n]; n++)
	{ if ( predicate_key[n] == f )
	  { if ( !get_predicate(db, pred, &p, q) )
	      return FALSE;
	    rc = unify_predicate_property(db, p, option, f, q);
	    goto out;
	  }
	}
	rc = PL_domain_error("rdf_predicate_property", option);
      } else
	rc = PL_type_error("rdf_predicate_property", option);
    out:
      close_query(q);
      return rc;
    }
    case PL_REDO:
      q = PL_foreign_context_address(h);
    redo:
      for( ; predicate_key[q->state.predprop.prop]; q->state.predprop.prop++ )
      { if ( unify_predicate_property(db,
				      q->state.predprop.pred,
				      option,
				      predicate_key[q->state.predprop.prop],
				      q) )
	{ q->state.predprop.prop++;
	  if ( predicate_key[q->state.predprop.prop] )
	    PL_retry_address(q);
	  return TRUE;
	}
      }
      return FALSE;
    case PL_PRUNED:
      q = PL_foreign_context_address(h);
      close_query(q);
      return TRUE;
    default:
      assert(0);
      return TRUE;
  }
}


		 /*******************************
		 *     TRANSITIVE RELATIONS	*
		 *******************************/

static visited *
alloc_node_agenda(rdf_db *db, agenda *a)
{ chunk *c;
  int size;

  if ( (c=a->chunk) )
  { if ( c->used < c->size )
    { visited *v = &c->nodes[c->used++];

      return v;
    }
  }

  size = (a->size == 0 ? 8 : 1024);
  c = rdf_malloc(db, CHUNK_SIZE(size));
  c->size = size;
  c->used = 1;
  c->next = a->chunk;
  a->chunk = c;

  return &c->nodes[0];
}


static void
empty_agenda(rdf_db *db, agenda *a)
{ chunk *c, *n;

  for(c=a->chunk; c; c = n)
  { n = c->next;
    rdf_free(db, c, CHUNK_SIZE(c->size));
  }
  if ( a->hash )
    rdf_free(db, a->hash, sizeof(visited*)*a->hash_size);

  if ( a->query )
    close_query(a->query);
}


static void
hash_agenda(rdf_db *db, agenda *a, int size)
{ if ( a->hash )
    rdf_free(db, a->hash, sizeof(*a->hash));
  if ( size > 0 )
  { visited *v;

    a->hash = rdf_malloc(db, sizeof(visited*)*size);
    memset(a->hash, 0, sizeof(visited*)*size);
    a->hash_size = size;

    for(v=a->head; v; v = v->next)
    { int key = atom_hash(v->resource, MURMUR_SEED)&(size-1);

      v->hash_link = a->hash[key];
      a->hash[key] = v;
    }
  }
}


static int
in_agenda(agenda *a, atom_t resource)
{ visited *v;

  if ( a->hash )
  { int key = atom_hash(resource, MURMUR_SEED)&(a->hash_size-1);
    v = a->hash[key];

    for( ; v; v = v->hash_link )
    { if ( v->resource == resource )
	return TRUE;
    }
  } else
  { v = a->head;

    for( ; v; v = v->next )
    { if ( v->resource == resource )
	return TRUE;
    }
  }

  return FALSE;
}


static visited *
append_agenda(rdf_db *db, agenda *a, atom_t res, uintptr_t d)
{ visited *v = a->head;

  if ( in_agenda(a, res) )
    return NULL;

  db->agenda_created++;			/* statistics */

  a->size++;
  if ( !a->hash_size && a->size > 32 )
    hash_agenda(db, a, 64);
  else if ( a->size > a->hash_size * 4 )
    hash_agenda(db, a, a->hash_size * 4);

  v = alloc_node_agenda(db, a);
  v->resource = res;
  v->distance = d;
  v->next = NULL;
  if ( a->tail )
  { a->tail->next = v;
    a->tail = v;
  } else
  { a->head = a->tail = v;
  }

  if ( a->hash_size )
  { int key = atom_hash(res, MURMUR_SEED)&(a->hash_size-1);

    v->hash_link = a->hash[key];
    a->hash[key] = v;
  }

  return v;
}


static int
can_reach_target(rdf_db *db, agenda *a, query *q)
{ triple_walker tw;
  int indexed = a->pattern.indexed;
  int rc = FALSE;
  triple *p;

  if ( indexed & BY_S )			/* subj ---> */
  { a->pattern.object.resource = a->target;
    indexed |= BY_O;
  } else
  { a->pattern.subject_id = ATOM_ID(a->target);
    indexed |= BY_S;
  }

  init_triple_walker(&tw, db, &a->pattern, indexed);
  while((p=next_triple(&tw)))
  { if ( match_triples(db, p, &a->pattern, q, MATCH_SUBPROPERTY) )
    { rc = TRUE;
      break;
    }
  }

  if ( a->pattern.indexed & BY_S )
  { a->pattern.object.resource = 0;
  } else
  { a->pattern.subject_id = 0;
  }

  return rc;
}



static visited *
bf_expand(rdf_db *db, agenda *a, atom_t resource, uintptr_t d, query *q)
{ search_state state;
  visited *rc = NULL;

  state.pattern = a->pattern;		/* Structure copy */
  state.flags   = MATCH_SUBPROPERTY|MATCH_INVERSE;
  state.p_cloud = NULL;
  state.query   = q;
  state.db      = db;

  if ( state.pattern.indexed & BY_S )		/* subj ---> */
  { state.pattern.subject_id = ATOM_ID(resource);
  } else
  { state.pattern.object.resource = resource;
  }

  if ( a->target && can_reach_target(db, a, q) )
    return append_agenda(db, a, a->target, d);

  for(;;)
  { int indexed = state.pattern.indexed;
    triple *p;

    init_triple_walker(&state.cursor, db, &state.pattern, indexed);
    while((p=next_triple(&state.cursor)))
    { if ( !alive_triple(a->query, p) )
	continue;

      if ( match_triples(db, p, &state.pattern, a->query, MATCH_SUBPROPERTY) )
      { atom_t found;
	visited *v;

	if ( indexed & BY_S )
	{ if ( p->object_is_literal )
	    continue;
	  found = p->object.resource;
	} else
	{ found = ID_ATOM(p->subject_id);
	}

	v = append_agenda(db, a, found, d);
	if ( !rc )
	  rc = v;
	if ( found == a->target )
	  return rc;
      }
    }
    if ( next_sub_property(&state) )
      continue;
    if ( inverse_partial_triple(&state.pattern) )
    { state.p_cloud = NULL;
      continue;
    }
    break;
  }
					/* TBD: handle owl:sameAs */
  return rc;
}


static int
peek_agenda(rdf_db *db, agenda *a)
{ if ( a->to_return )
    return TRUE;

  while( a->to_expand )
  { uintptr_t next_d = a->to_expand->distance+1;

    if ( next_d > a->max_d )
      return FALSE;

    a->to_return = bf_expand(db, a,
			     a->to_expand->resource,
			     next_d,
			     a->query);
    a->to_expand = a->to_expand->next;

    if ( a->to_return )
      return TRUE;
  }

  return FALSE;
}


static visited *
next_agenda(rdf_db *db, agenda *a)
{ if ( peek_agenda(db, a) )
  { visited *v = a->to_return;

    a->to_return = a->to_return->next;

    return v;
  }

  return NULL;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
rdf_reachable(+Subject, +Predicate, -Object)
rdf_reachable(-Subject, +Predicate, ?Object)
    Examine transitive relations, reporting all `Object' that can be
    reached from `Subject' using Predicate without going into a loop
    if the relation is cyclic.

directly_attached() deals with the posibility that  the predicate is not
defined and Subject and Object are  the   same.  Should  use clean error
handling, but that means a lot of changes. For now this will do.

TBD:	Implement bi-directional search if both Subject and Object are
	given.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
directly_attached(term_t pred, term_t from, term_t to)
{ if ( PL_is_atom(pred) && PL_is_atom(from) )
    return PL_unify(to, from);

  return FALSE;
}


static int
unify_distance(term_t d, uintptr_t dist)
{ if ( d )
    return PL_unify_integer(d, dist);

  return TRUE;
}


static foreign_t
rdf_reachable(term_t subj, term_t pred, term_t obj,
	      term_t max_d, term_t d,
	      control_t h)
{ rdf_db *db = rdf_current_db();
  query *q;

  switch(PL_foreign_control(h))
  { case PL_FIRST_CALL:
    { visited *v;
      agenda *a;
      term_t target_term;
      int is_det = FALSE;

      if ( PL_is_variable(pred) )
	return PL_instantiation_error(pred);

      if ( !(q = open_query(db)) )
	return FALSE;
      a = &q->state.tr_search;
      memset(a, 0, sizeof(*a));
      a->query = q;

      if ( max_d )
      { long md;
	atom_t inf;

	if ( PL_get_atom(max_d, &inf) && inf == ATOM_infinite )
	{ a->max_d = (uintptr_t)-1;
	} else
	{ if ( !PL_get_long_ex(max_d, &md) || md < 0 )
	  { close_query(q);
	    return FALSE;
	  }
	  a->max_d = md;
	}
      } else
      { a->max_d = (uintptr_t)-1;
      }

      if ( !PL_is_variable(subj) )		/* subj .... obj */
      { switch(get_partial_triple(db, subj, pred, 0, 0, &a->pattern))
	{ case 0:
	  { close_query(q);
	    return directly_attached(pred, subj, obj) &&
		   unify_distance(d, 0);
	  }
	  case -1:
	  { close_query(q);
	    return FALSE;
	  }
	}
	is_det = PL_is_ground(obj);
	if ( a->pattern.object_is_literal )
	{ close_query(q);
	  return FALSE;			/* rdf_reachable(literal(...),?,?) */
	}
	target_term = obj;
      } else if ( !PL_is_variable(obj) )	/* obj .... subj */
      {	switch(get_partial_triple(db, 0, pred, obj, 0, &a->pattern))
	{ case 0:
	  { close_query(q);
	    return directly_attached(pred, obj, subj);
	  }
	  case -1:
	  { close_query(q);
	    return FALSE;
	  }
	}
	if ( a->pattern.object_is_literal )
	{ close_query(q);
	  return FALSE;			/* rdf_reachable(-,+,literal(...)) */
	}
	target_term = subj;
      } else
      { close_query(q);
	return PL_instantiation_error(subj);
      }

      if ( (a->pattern.indexed & BY_S) )		/* subj ... */
	append_agenda(db, a, ID_ATOM(a->pattern.subject_id), 0);
      else
	append_agenda(db, a, a->pattern.object.resource, 0);
      a->to_return = a->head;
      a->to_expand = a->head;

      while( (v=next_agenda(db, a)) )
      { if ( PL_unify_atom(target_term, v->resource) )
	{ if ( is_det )		/* mode(+, +, +) */
	  { int rc = unify_distance(d, v->distance);
	    empty_agenda(db, a);
	    return rc;
	  } else if ( unify_distance(d, v->distance) )
	  {				/* mode(+, +, -) or mode(-, +, +) */
	    if ( peek_agenda(db, a) )
	      PL_retry_address(a);

	    empty_agenda(db, a);
	    return TRUE;
	  }
	}
      }
      empty_agenda(db, a);
      return FALSE;
    }
    case PL_REDO:
    { agenda *a = PL_foreign_context_address(h);
      term_t target_term;
      visited *v;

      if ( !PL_is_variable(subj) )	/* +, +, - */
	target_term = obj;
      else
	target_term = subj;		/* -, +, + */

      while( (v=next_agenda(db, a)) )
      { if ( PL_unify_atom(target_term, v->resource) &&
	     unify_distance(d, v->distance) )
	{ if ( peek_agenda(db, a) )
	  { PL_retry_address(a);
	  } else
	  { empty_agenda(db, a);
	    return TRUE;
	  }
	}
      }

      empty_agenda(db, a);
      return FALSE;
    }
    case PL_PRUNED:
    { agenda *a = PL_foreign_context_address(h);

      DEBUG(9, Sdprintf("Cutted; agenda = %p\n", a));

      empty_agenda(db, a);
      return TRUE;
    }
    default:
      assert(0);
      return FALSE;
  }
}

static foreign_t
rdf_reachable3(term_t subj, term_t pred, term_t obj, control_t h)
{ return rdf_reachable(subj, pred, obj, 0, 0, h);
}

static foreign_t
rdf_reachable5(term_t subj, term_t pred, term_t obj, term_t max_d, term_t d,
	       control_t h)
{ return rdf_reachable(subj, pred, obj, max_d, d, h);
}


		 /*******************************
		 *	     STATISTICS		*
		 *******************************/

static functor_t keys[16];		/* initialised in install_rdf_db() */

static int
unify_statistics(rdf_db *db, term_t key, functor_t f)
{ int64_t v;

  if ( f == FUNCTOR_triples1 )
  { v = db->created - db->erased;
  } else if ( f == FUNCTOR_resources1 )
  { v = db->resources.hash.count;
  } else if ( f == FUNCTOR_predicates1 )
  { v = db->predicates.count;
  } else if ( f == FUNCTOR_graphs1 )
  { v = db->graphs.count - db->graphs.erased;
  } else if ( f == FUNCTOR_indexed16 )
  { int i;
    term_t a = PL_new_term_ref();

    if ( !PL_unify_functor(key, FUNCTOR_indexed16) )
      return FALSE;
    for(i=0; i<16; i++)
    { if ( !PL_get_arg(i+1, key, a) ||
	   !PL_unify_integer(a, db->indexed[i]) )
	return FALSE;
    }

    return TRUE;
  } else if ( f == FUNCTOR_hash_quality1 )
  { term_t tail, list = PL_new_term_ref();
    term_t head = PL_new_term_ref();
    term_t tmp = PL_new_term_ref();
    term_t av = PL_new_term_refs(4);
    int i;

    if ( !PL_unify_functor(key, FUNCTOR_hash_quality1) )
      return FALSE;
    _PL_get_arg(1, key, list);
    tail = PL_copy_term_ref(list);

    for(i=1; i<INDEX_TABLES; i++)
    { if ( db->hash[i].created )
      { if ( !PL_unify_list(tail, head, tail) ||
	     !PL_put_integer(av+0, col_index[i]) ||
	     !PL_put_integer(av+1, db->hash[i].bucket_count) ||
	     !PL_put_float(av+2, triple_hash_quality(db, i, 1024)) ||
	     !PL_put_integer(av+3, MSB(db->hash[i].bucket_count)-
				   MSB(db->hash[i].bucket_count_epoch)) ||
	     !PL_cons_functor_v(tmp, FUNCTOR_hash4, av) ||
	     !PL_unify(head, tmp) )
	  return FALSE;
      }
    }

    return PL_unify_nil(tail);
  } else if ( f == FUNCTOR_searched_nodes1 )
  { v = db->agenda_created;
  } else if ( f == FUNCTOR_duplicates1 )
  { if ( db->duplicates_up_to_date == FALSE )
      return FALSE;
    v = db->duplicates;
  } else if ( f == FUNCTOR_lingering1 )
  { v = db->lingering;
  } else if ( f == FUNCTOR_literals1 )
  { v = db->literals.count;
  } else if ( f == FUNCTOR_triples2 && PL_is_functor(key, f) )
  { graph *src;
    term_t a = PL_new_term_ref();
    atom_t name;

    _PL_get_arg(1, key, a);
    if ( !PL_get_atom_ex(a, &name) )
      return FALSE;
    if ( (src = existing_graph(db, name)) )
      v = src->triple_count;
    else
      v = 0;

    _PL_get_arg(2, key, a);
    return PL_unify_int64(a, v);
  } else if ( f == FUNCTOR_gc4 )
  { return PL_unify_term(key,
			 PL_FUNCTOR, f,
			   PL_INT,   (int)db->gc.count,
			   PL_INT64, (int64_t)db->gc.reclaimed_triples,
			   PL_INT64, (int64_t)db->reindexed,
			   PL_FLOAT, (double)db->gc.time);	/* time spent */
  } else
  { assert(0);
    return FALSE;
  }

  return PL_unify_term(key, PL_FUNCTOR, f, PL_INT64, v);
}

static foreign_t
rdf_statistics(term_t key, control_t h)
{ int n;
  rdf_db *db = rdf_current_db();

  switch(PL_foreign_control(h))
  { case PL_FIRST_CALL:
    { functor_t f;

      if ( PL_is_variable(key) )
      { n = 0;
	goto redo;
      } else if ( PL_get_functor(key, &f) )
      { for(n=0; keys[n]; n++)
	{ if ( keys[n] == f )
	    return unify_statistics(db, key, f);
	}
	return PL_domain_error("rdf_statistics", key);
      } else
	return PL_type_error("rdf_statistics", key);
    }
    case PL_REDO:
      n = (int)PL_foreign_context(h);
    redo:
      unify_statistics(db, key, keys[n]);
      n++;
      if ( keys[n] )
	PL_retry(n);
    case PL_PRUNED:
      return TRUE;
    default:
      assert(0);
      return TRUE;
  }
}


/** rdf_generation(-Generation) is det.

    True when Generation is the current reading generation.  If we are
    inside a modified transaction, Generation has the format Base+TrGen,
    where TrGen expresses the generation inside the transaction.
*/

static foreign_t
rdf_generation(term_t t)
{ rdf_db *db = rdf_current_db();
  query *q = open_query(db);
  int rc;

  if ( !q ) return FALSE;
  if ( q->tr_gen > q->stack->tr_gen_base )
  { assert(q->tr_gen < q->stack->tr_gen_max);

    rc = PL_unify_term(t, PL_FUNCTOR, FUNCTOR_plus2,
		            PL_INT64, q->rd_gen,
		            PL_INT64, q->tr_gen - q->stack->tr_gen_base);
  } else
  { rc = PL_unify_int64(t, q->rd_gen);
  }

  close_query(q);

  return rc;
}


/** rdf_snapshot(-Snapshot) is det.

    True when Snapshot is a handle to the current state of the database.
*/

static foreign_t
rdf_snapshot(term_t t)
{ rdf_db *db = rdf_current_db();
  snapshot *s = new_snapshot(db);

  if ( !s )
    return FALSE;
  return unify_snapshot(t, s);
}


		 /*******************************
		 *	  CONTROL INDEXING	*
		 *******************************/

/** rdf_set(+What)

    Set aspect of the RDF database.  What is one of:

      * hash(Which, Parameter, Value)

    Where Parameter is one of =size=, =optimize_threshold= or
    =avg_chain_len= and Which is one of =s=, =p=, etc.
*/

static int
get_index_name(term_t t, int *index)
{ int i;
  char *s;

  if ( !PL_get_chars(t, &s, CVT_ATOM|CVT_EXCEPTION) )
    return FALSE;

  for(i=1; i<INDEX_TABLES; i++)
  { if ( strcmp(s, col_name[i]) == 0 )
    { *index = i;
      return TRUE;
    }
  }

  PL_domain_error("index", t);
  return FALSE;
}


static foreign_t
rdf_set(term_t what)
{ rdf_db *db = rdf_current_db();

  if ( PL_is_functor(what, FUNCTOR_hash3) )
  { term_t arg = PL_new_term_ref();
    int index;
    int value;
    atom_t param;

    _PL_get_arg(1, what, arg);
    if ( !get_index_name(arg, &index) )
      return FALSE;

    _PL_get_arg(3, what, arg);
    if ( !PL_get_integer_ex(arg, &value) )
      return FALSE;

    _PL_get_arg(2, what, arg);
    if ( !PL_get_atom_ex(arg, &param) )
      return FALSE;

    if ( param == ATOM_size )
    { if ( size_triple_hash(db, index, value) )
      { db->hash[index].user_size = MSB(value);
	return TRUE;
      }
      if ( value <= 0 || MSB(value) >= MAX_TBLOCKS )
	return PL_domain_error("hash_size", arg);
						/* cannot shrink */
      return PL_permission_error("size", "hash", arg);
    } else if ( param == ATOM_optimize_threshold )
    { if ( value >= 0 && value < 20 )
	db->hash[index].optimize_threshold = value;
      else
	return PL_domain_error("optimize_threshold", arg);
    } else if ( param == ATOM_average_chain_len )
    { if ( value >= 0 && value < 20 )
	db->hash[index].avg_chain_len = value;
      return PL_domain_error("average_chain_len", arg);
    } else
      return PL_domain_error("rdf_hash_parameter", arg);

    return TRUE;
  }

  return PL_type_error("rdf_setting", what);
}


static foreign_t
rdf_update_duplicates(void)
{ rdf_db *db = rdf_current_db();

  return update_duplicates(db);
}


/** rdf_warm_indexes(+List) is det.
*/

static foreign_t
rdf_warm_indexes(term_t indexes)
{ int il[16];
  int ic = 0;
  term_t tail = PL_copy_term_ref(indexes);
  term_t head = PL_new_term_ref();
  rdf_db *db = rdf_current_db();

  while(PL_get_list_ex(tail, head, tail))
  { char *s;

    if ( PL_get_chars(head, &s, CVT_ATOM|CVT_STRING|CVT_EXCEPTION) )
    { int by = 0;
      int i;

      for(; *s; s++)
      { switch(*s)
	{ case 's': by |= BY_S; break;
	  case 'p': by |= BY_P; break;
	  case 'o': by |= BY_O; break;
	  case 'g': by |= BY_G; break;
	  default: return PL_domain_error("rdf_index", head);
	}
      }

      if ( index_col[by] == ~0 )
	return PL_existence_error("rdf_index", head);

      for(i=0; i<ic; i++)
      { if ( il[i] == by )
	  break;
      }
      if ( i == ic )
	il[ic++] = ICOL(by);
    } else
      return 0;
  }
  if ( !PL_get_nil_ex(tail) )
    return FALSE;

  create_triple_hashes(db, ic, il);

  return TRUE;
}


static foreign_t
pl_empty_prefix_table(void)
{ rdf_db *db = rdf_current_db();

  empty_prefix_table(db);

  return TRUE;
}


		 /*******************************
		 *	       RESET		*
		 *******************************/

static void
erase_triples(rdf_db *db)
{ triple *t, *n;
  int i;

  for(t=fetch_triple(db, db->by_none.head); t; t=n)
  { n = triple_follow_hash(db, t, ICOL(BY_NONE));

    free_triple(db, t, FALSE);		/* ? */
  }
  db->by_none.head = db->by_none.tail = 0;

  for(i=BY_S; i<INDEX_TABLES; i++)
  { triple_hash *hash = &db->hash[i];

    reset_triple_hash(db, hash);
  }
  reset_triple_array(db);

  db->created = 0;
  db->erased = 0;
  memset(db->indexed, 0, sizeof(db->indexed));
  db->duplicates = 0;
  db->queries.generation = 0;
}


static void
erase_predicates(rdf_db *db)
{ int i;

  for(i=0; i<db->predicates.bucket_count; i++)
  { predicate *n, *p = db->predicates.blocks[MSB(i)][i];

    db->predicates.blocks[MSB(i)][i] = NULL;

    for( ; p; p = n )
    { n = p->next;

      free_list(db, &p->subPropertyOf);
      free_list(db, &p->siblings);
      if ( ++p->cloud->deleted == p->cloud->size )
	free_predicate_cloud(db, p->cloud);
      free_is_leaf(db, p);

      rdf_free(db, p, sizeof(*p));
    }
  }

  db->predicates.count = 0;
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Reset the DB. It might be wiser to create  a new one and have a seperate
thread deleting the old one (e.g. do this in GC).
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
reset_db(rdf_db *db)
{ int rc;

  suspend_gc(db);
  simpleMutexLock(&db->locks.duplicates);
  erase_snapshots(db);
  erase_triples(db);
  erase_predicates(db);
  erase_resources(&db->resources);
  erase_graphs(db);
  empty_prefix_table(db);
  db->agenda_created = 0;
  skiplist_destroy(&db->literals);

  rc = (init_resource_db(db, &db->resources) &&
	init_literal_table(db));

  db->snapshots.keep = GEN_MAX;
  db->queries.generation = GEN_EPOCH;

  simpleMutexUnlock(&db->locks.duplicates);
  resume_gc(db);

  return rc;
}


/** rdf_reset_db

    Reset the RDF database to its initial state.  Only allowed if there
    are no active queries. This means that if the calling thread has
    open queries this must be considered a permission error.  Otherwise
    we wait until all queries have died.

    TBD: Check queries in other threads!
*/

static foreign_t
rdf_reset_db(void)
{ rdf_db *db = rdf_current_db();
  query *q;
  int rc;

  db->resetting = TRUE;
  if ( !(q = open_query(db)) )
    return FALSE;

  if ( q->depth > 0 || q->transaction )
  { close_query(q);
    return permission_error("reset", "rdf_db", "default",
			    "Active queries");
  }

  if ( !rdf_broadcast(EV_RESET, NULL, NULL) )
    return FALSE;

  rc = reset_db(db);
  close_query(q);
  db->resetting = FALSE;

  return rc;
}


static foreign_t
rdf_delete_snapshot(term_t t)
{ snapshot *ss;
  int rc;

  if ( (rc=get_snapshot(t, &ss)) == TRUE )
  { if ( free_snapshot(ss) )
      return TRUE;
    rc = -1;
  }

  if ( rc == -1 )
    return PL_existence_error("rdf_snapshot", t);

  return PL_type_error("rdf_snapshot", t);
}

#ifdef O_DEBUG
static foreign_t
rdf_checks_literal_references(term_t l)
{ triple p, *t;
  triple_walker tw;
  long count = 0, refs = -1;
  term_t var = PL_new_term_ref();
  rdf_db *db = rdf_current_db();

  memset(&p, 0, sizeof(p));
  if ( !get_partial_triple(db, var, var, l, 0, &p) )
    return FALSE;
  assert(p.object_is_literal);

  init_triple_walker(&tw, db, &p, BY_O);
  while((t=next_triple(&tw)))
  { if ( match_object(t, &p, MATCH_QUAL) )
    { if ( count++ == 0 )
      { refs = (long)t->object.literal->references;
      }
    }
  }
  destroy_triple_walker(db, &tw);

  if ( count != refs )
  { if ( refs == -1 )
    { Sdprintf("Not found in triples\n");
    } else
    { Sdprintf("Refs: %ld; counted: %ld; lit=", refs, count);
      print_literal(p.object.literal);
      Sdprintf("\n");
    }

    return FALSE;
  }

  return TRUE;
}
#endif

		 /*******************************
		 *	       MATCH		*
		 *******************************/


static int
get_text_ex(term_t term, text *txt)
{ memset(txt, 0, sizeof(*txt));

  return ( PL_get_nchars(term, &txt->length, (char**)&txt->a,
			 CVT_ATOM|CVT_STRING) ||
	   PL_get_wchars(term, &txt->length, (pl_wchar_t**)&txt->w,
			 CVT_ATOM|CVT_STRING|CVT_EXCEPTION)
	 );
}



static foreign_t
match_label(term_t how, term_t search, term_t label)
{ atom_t h;
  text f, l;
  int type;

  if ( !PL_get_atom_ex(how, &h) ||
       !get_text_ex(search, &f) ||
       !get_text_ex(label, &l) )
    return FALSE;

  if ( h == ATOM_exact )
    type = STR_MATCH_ICASE;
  if ( h == ATOM_icase )
    type = STR_MATCH_ICASE;
  else if ( h == ATOM_substring )
    type = STR_MATCH_SUBSTRING;
  else if ( h == ATOM_word )
    type = STR_MATCH_WORD;
  else if ( h == ATOM_prefix )
    type = STR_MATCH_PREFIX;
  else if ( h == ATOM_like )
    type = STR_MATCH_LIKE;
  else
    return PL_domain_error("search_method", how);

  return match_text(type, &f, &l);
}


static foreign_t
lang_matches(term_t lang, term_t pattern)
{ atom_t l, p;

  if ( !PL_get_atom_ex(lang, &l) ||
       !PL_get_atom_ex(pattern, &p) )
    return FALSE;

  return atom_lang_matches(l, p);
}


static foreign_t
rdf_compare(term_t dif, term_t a, term_t b)
{ triple ta, tb;
  rdf_db *db = rdf_current_db();
  int rc;

  memset(&ta, 0, sizeof(ta));
  memset(&tb, 0, sizeof(tb));
  if ( get_object(db, a, &ta) &&
       get_object(db, b, &tb) )
  { int d;
    atom_t ad;

    if ( ta.object_is_literal &&
	 tb.object_is_literal )
    { literal_ex lex;
      lex.literal = ta.object.literal;
      prepare_literal_ex(&lex);
      d = compare_literals(&lex, tb.object.literal);
    } else if ( !ta.object_is_literal && !tb.object_is_literal )
    { d = cmp_atoms(ta.object.resource, tb.object.resource);
    } else
    { d = ta.object_is_literal ? -1 : 1;
    }

    ad = d < 0 ? ATOM_lt : d > 0 ? ATOM_gt : ATOM_eq;

    rc = PL_unify_atom(dif, ad);
  } else
  { rc = FALSE;
  }

  free_triple(db, &ta, FALSE);
  free_triple(db, &tb, FALSE);

  return rc;
}


		 /*******************************
		 *	       TEST		*
		 *******************************/

static foreign_t
rdf_is_bnode(term_t t)
{ size_t len;
  char *s;

  if ( PL_get_nchars(t, &len, &s, CVT_ATOM) &&
       s[0] == '_' && (s[1] == ':' || s[1] == '_') )
    return TRUE;

  return FALSE;
}


		 /*******************************
		 *	       VERSION		*
		 *******************************/

static foreign_t
rdf_version(term_t v)
{ return PL_unify_integer(v, RDF_VERSION);
}


		 /*******************************
		 *	     REGISTER		*
		 *******************************/

#define MKFUNCTOR(n, a) \
	FUNCTOR_ ## n ## a = PL_new_functor(PL_new_atom(#n), a)
#define NDET PL_FA_NONDETERMINISTIC
#define META PL_FA_TRANSPARENT

install_t
install_rdf_db(void)
{ int i=0;
  extern install_t install_atom_map(void);

  simpleMutexInit(&rdf_lock);
  init_errors();
  register_resource_predicates();

  MKFUNCTOR(literal, 1);
  MKFUNCTOR(triples, 1);
  MKFUNCTOR(triples, 2);
  MKFUNCTOR(resources, 1);
  MKFUNCTOR(predicates, 1);
  MKFUNCTOR(subject, 1);
  MKFUNCTOR(predicate, 1);
  MKFUNCTOR(object, 1);
  MKFUNCTOR(graph, 1);
  MKFUNCTOR(indexed, 16);
  MKFUNCTOR(exact, 1);
  MKFUNCTOR(icase, 1);
  MKFUNCTOR(plain, 1);
  MKFUNCTOR(substring, 1);
  MKFUNCTOR(word, 1);
  MKFUNCTOR(prefix, 1);
  MKFUNCTOR(like, 1);
  MKFUNCTOR(lt, 1);
  MKFUNCTOR(le, 1);
  MKFUNCTOR(between, 2);
  MKFUNCTOR(eq, 1);
  MKFUNCTOR(ge, 1);
  MKFUNCTOR(gt, 1);
  MKFUNCTOR(literal, 2);
  MKFUNCTOR(searched_nodes, 1);
  MKFUNCTOR(duplicates, 1);
  MKFUNCTOR(lingering, 1);
  MKFUNCTOR(literals, 1);
  MKFUNCTOR(symmetric, 1);
  MKFUNCTOR(transitive, 1);
  MKFUNCTOR(inverse_of, 1);
  MKFUNCTOR(lang, 2);
  MKFUNCTOR(type, 2);
  MKFUNCTOR(rdf_subject_branch_factor, 1);
  MKFUNCTOR(rdf_object_branch_factor, 1);
  MKFUNCTOR(rdfs_subject_branch_factor, 1);
  MKFUNCTOR(rdfs_object_branch_factor, 1);
  MKFUNCTOR(gc, 4);
  MKFUNCTOR(graphs, 1);
  MKFUNCTOR(assert, 4);
  MKFUNCTOR(retract, 4);
  MKFUNCTOR(update, 5);
  MKFUNCTOR(new_literal, 1);
  MKFUNCTOR(old_literal, 1);
  MKFUNCTOR(transaction, 2);
  MKFUNCTOR(load, 2);
  MKFUNCTOR(begin, 1);
  MKFUNCTOR(end, 1);
  MKFUNCTOR(create_graph, 1);
  MKFUNCTOR(hash_quality, 1);
  MKFUNCTOR(hash, 3);
  MKFUNCTOR(hash, 4);

  FUNCTOR_colon2 = PL_new_functor(PL_new_atom(":"), 2);
  FUNCTOR_plus2  = PL_new_functor(PL_new_atom("+"), 2);

  ATOM_user		  = PL_new_atom("user");
  ATOM_exact		  = PL_new_atom("exact");
  ATOM_icase		  = PL_new_atom("icase");
  ATOM_plain		  = PL_new_atom("plain");
  ATOM_prefix		  = PL_new_atom("prefix");
  ATOM_like		  = PL_new_atom("like");
  ATOM_substring	  = PL_new_atom("substring");
  ATOM_word		  = PL_new_atom("word");
  ATOM_subPropertyOf	  = PL_new_atom(URL_subPropertyOf);
  ATOM_xsdString	  = PL_new_atom(URL_xsdString);
  ATOM_xsdDouble	  = PL_new_atom(URL_xsdDouble);
  ATOM_error		  = PL_new_atom("error");
  ATOM_begin		  = PL_new_atom("begin");
  ATOM_end		  = PL_new_atom("end");
  ATOM_error		  = PL_new_atom("error");
  ATOM_infinite		  = PL_new_atom("infinite");
  ATOM_snapshot		  = PL_new_atom("snapshot");
  ATOM_true		  = PL_new_atom("true");
  ATOM_size		  = PL_new_atom("size");
  ATOM_optimize_threshold = PL_new_atom("optimize_threshold");
  ATOM_average_chain_len  = PL_new_atom("average_chain_len");
  ATOM_reset		  = PL_new_atom("reset");
  ATOM_lt		  = PL_new_atom("<");
  ATOM_eq		  = PL_new_atom("=");
  ATOM_gt		  = PL_new_atom(">");

  PRED_call1         = PL_predicate("call", 1, "user");

					/* statistics */
  keys[i++] = FUNCTOR_graphs1;
  keys[i++] = FUNCTOR_triples1;
  keys[i++] = FUNCTOR_resources1;
  keys[i++] = FUNCTOR_indexed16;
  keys[i++] = FUNCTOR_hash_quality1;
  keys[i++] = FUNCTOR_predicates1;
  keys[i++] = FUNCTOR_searched_nodes1;
  keys[i++] = FUNCTOR_duplicates1;
  keys[i++] = FUNCTOR_lingering1;
  keys[i++] = FUNCTOR_literals1;
  keys[i++] = FUNCTOR_triples2;
  keys[i++] = FUNCTOR_gc4;
  keys[i++] = 0;
  assert(i<=16);

  check_index_tables();
					/* see struct triple */
  assert(sizeof(literal) <= sizeof(triple*)*INDEX_TABLES);

  PL_register_foreign("rdf_version",    1, rdf_version,     0);
  PL_register_foreign("rdf_assert",	3, rdf_assert3,	    0);
  PL_register_foreign("rdf_assert",	4, rdf_assert4,	    0);
  PL_register_foreign("rdf_update",	4, rdf_update,      0);
  PL_register_foreign("rdf_update",	5, rdf_update5,     0);
  PL_register_foreign("rdf_retractall",	3, rdf_retractall3, 0);
  PL_register_foreign("rdf_retractall",	4, rdf_retractall4, 0);
  PL_register_foreign("rdf",		3, rdf3,	    NDET);
  PL_register_foreign("rdf",		4, rdf4,	    NDET);
  PL_register_foreign("rdf_has",	4, rdf_has4,	    NDET);
  PL_register_foreign("rdf_has",	3, rdf_has3,	    NDET);
  PL_register_foreign("rdf_gc_",	0, rdf_gc,	    0);
  PL_register_foreign("rdf_add_gc_time",1, rdf_add_gc_time, 0);
  PL_register_foreign("rdf_gc_info_",   1, rdf_gc_info,	    0);
  PL_register_foreign("rdf_statistics_",1, rdf_statistics,  NDET);
  PL_register_foreign("rdf_set",        1, rdf_set,         0);
  PL_register_foreign("rdf_update_duplicates",
					0, rdf_update_duplicates, 0);
  PL_register_foreign("rdf_warm_indexes",
					1, rdf_warm_indexes,0);
  PL_register_foreign("rdf_generation", 1, rdf_generation,  0);
  PL_register_foreign("rdf_snapshot",   1, rdf_snapshot,    0);
  PL_register_foreign("rdf_delete_snapshot", 1, rdf_delete_snapshot, 0);
  PL_register_foreign("rdf_match_label",3, match_label,     0);
  PL_register_foreign("rdf_save_db_",   3, rdf_save_db,     0);
  PL_register_foreign("rdf_load_db_",   3, rdf_load_db,     0);
  PL_register_foreign("rdf_reachable",  3, rdf_reachable3,  NDET);
  PL_register_foreign("rdf_reachable",  5, rdf_reachable5,  NDET);
  PL_register_foreign("rdf_reset_db_",  0, rdf_reset_db,    0);
  PL_register_foreign("rdf_set_predicate",
					2, rdf_set_predicate, 0);
  PL_register_foreign("rdf_predicate_property_",
					2, rdf_predicate_property, NDET);
  PL_register_foreign("rdf_current_predicate",
					1, rdf_current_predicate, NDET);
  PL_register_foreign("rdf_current_literal",
					1, rdf_current_literal, NDET);
  PL_register_foreign("rdf_graph_",     2, rdf_graph,       NDET);
  PL_register_foreign("rdf_create_graph",  1, rdf_create_graph, 0);
  PL_register_foreign("rdf_destroy_graph", 1, rdf_destroy_graph, 0);
  PL_register_foreign("rdf_set_graph_source", 3, rdf_set_graph_source, 0);
  PL_register_foreign("rdf_graph_source_", 3, rdf_graph_source, 0);
  PL_register_foreign("rdf_estimate_complexity",
					4, rdf_estimate_complexity, 0);
  PL_register_foreign("rdf_transaction", 3, rdf_transaction, META);
  PL_register_foreign("rdf_active_transactions_",
					1, rdf_active_transactions, 0);
  PL_register_foreign("rdf_monitor_",   2, rdf_monitor,     META);
  PL_register_foreign("rdf_empty_prefix_cache",
					0, pl_empty_prefix_table, 0);
  PL_register_foreign("rdf_is_bnode",   1, rdf_is_bnode,    0);
#ifdef WITH_MD5
  PL_register_foreign("rdf_md5",	2, rdf_md5,	    0);
  PL_register_foreign("rdf_graph_modified_", 3, rdf_graph_modified_, 0);
  PL_register_foreign("rdf_graph_clear_modified_",
				        1, rdf_graph_clear_modified_, 0);
  PL_register_foreign("rdf_atom_md5",	3, rdf_atom_md5,    0);
#endif

#ifdef O_DEBUG
  PL_register_foreign("rdf_debug",      1, rdf_debug,       0);
  PL_register_foreign("rdf_print_predicate_cloud", 2,
		      rdf_print_predicate_cloud, 0);
  PL_register_foreign("rdf_checks_literal_references", 1,
		      rdf_checks_literal_references, 0);
#endif

  PL_register_foreign("lang_matches", 2, lang_matches, 0);
  PL_register_foreign("rdf_compare",  3, rdf_compare,  0);

  install_atom_map();
}
