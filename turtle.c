/*  Part of SWI-Prologs

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2009-2017, VU University Amsterdam
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

#include <config.h>
#if defined(__sun) || __ENVIRONMENT_MAC_OS_X_VERSION_MIN_REQUIRED__ < 1070
#undef HAVE_WCSDUP			/* there is no prototype */
#undef HAVE_WCSCASECMP			/* same problem */
#endif

#ifdef HAVE_VISIBILITY_ATTRIBUTE
#define SO_LOCAL __attribute__((visibility("hidden")))
#else
#define SO_LOCAL
#endif
#define COMMON(type) SO_LOCAL type

#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <wchar.h>
#include <assert.h>
#include "murmur.h"
#include "turtle_chars.c"

#ifdef __WINDOWS__
#define swprintf _snwprintf
#endif

static functor_t FUNCTOR_error2;
static functor_t FUNCTOR_existence_error2;
static functor_t FUNCTOR_syntax_error1;
static functor_t FUNCTOR_stream4;
static functor_t FUNCTOR_node1;
static functor_t FUNCTOR_rdf3;
static functor_t FUNCTOR_rdf4;
static functor_t FUNCTOR_literal1;
static functor_t FUNCTOR_lang2;
static functor_t FUNCTOR_type2;
static functor_t FUNCTOR_pair2;
static functor_t FUNCTOR_colon2;

static atom_t ATOM_parse;
static atom_t ATOM_statement;
static atom_t ATOM_document;
static atom_t ATOM_count;
static atom_t ATOM_error_count;
static atom_t ATOM_anon_prefix;
static atom_t ATOM_base_uri;
static atom_t ATOM_on_error;
static atom_t ATOM_error;
static atom_t ATOM_warning;
static atom_t ATOM_format;
static atom_t ATOM_turtle;
static atom_t ATOM_trig;
static atom_t ATOM_graph;
static atom_t ATOM_auto;

#define RDF_NS L"http://www.w3.org/1999/02/22-rdf-syntax-ns#"
#define XSD_NS L"http://www.w3.org/2001/XMLSchema#"

#define RDF_TYPE    (RDF_NS L"type")
#define RDF_FIRST   (RDF_NS L"first")
#define RDF_REST    (RDF_NS L"rest")
#define RDF_NIL     (RDF_NS L"nil")
#define XSD_INTEGER (XSD_NS L"integer")
#define XSD_DECIMAL (XSD_NS L"decimal")
#define XSD_DOUBLE  (XSD_NS L"double")
#define XSD_BOOLEAN (XSD_NS L"boolean")


		 /*******************************
		 *	    PORTABILITY		*
		 *******************************/

#ifndef HAVE_WCSDUP
static wchar_t *
my_wcsdup(const wchar_t *in)
{ wchar_t *copy = malloc((wcslen(in)+1)*sizeof(wchar_t));

  if ( copy )
    return wcscpy(copy, in);

  return NULL;
}
#define wcsdup(ws) my_wcsdup(ws)
#endif

#ifndef HAVE_WCSCASECMP
#include <wctype.h>
static int
my_wcscasecmp(const wchar_t *s1, const wchar_t *s2)
{ wint_t n1, n2;

  if (s1 == s2)
    return 0;

  do
  { n1 = towlower(*s1++);
    n2 = towlower(*s2++);
    if (n1 == L'\0')
      break;
  } while (n1 == n2);

  return n1 - n2;
}
#define wcscasecmp(s1, s2) my_wcscasecmp(s1, s2)
#endif


		 /*******************************
		 *	 DATA STRUCTURES	*
		 *******************************/

#define FAST_URI 128			/* Avoid malloc for shorted ones */

typedef enum resource_type
{ R_BNODE,
  R_RESOURCE
} resource_type;

typedef enum object_type
{ O_RESOURCE,
  O_LITERAL
} object_type;

typedef enum error_mode
{ E_WARNING = 0,			/* default */
  E_ERROR
} error_mode;

typedef enum format
{ D_AUTO = 0,
  D_TURTLE,
  D_TRIG,
  D_TRIG_NO_GRAPH			/* read TRiG as Turtle */
} format;

typedef enum recover_mode
{ R_STATEMENT = 0,			/* on error, re-sync at statement */
  R_PREDOBJLIST,			/* on error, re-sync at ; or . */
  R_OBJLIST				/* on error, re-sync at , ; or . */
} recover_mode;

typedef struct resource
{ resource_type  type;
  int		 constant;		/* read-only constant */
  union
  { struct
    { wchar_t   *name;			/* Name of the IRI */
      atom_t	 handle;		/* Handle to Prolog atom */
      wchar_t	 fast[FAST_URI];	/* Buffer for short URIs */
    } r;
    size_t bnode_id;
    struct resource *next;		/* Next in free list */
  } v;
} resource;

typedef struct object
{ object_type	type;
  union
  { resource *r;
    struct
    { size_t	len;
      wchar_t  *string;
      wchar_t  *lang;
      resource *type;
    } l;
  } value;
} object;

typedef struct hash_cell
{ wchar_t	*name;			/* Name of the entry */
  struct hash_cell *next;		/* Hash link */
  struct
  { wchar_t *s;				/* Value as a string */
    size_t   bnode_id;			/* Blank node id */
  } value;
} hash_cell;

typedef struct hash_map
{ size_t	count;			/* number of entries in map */
  size_t	size;			/* Size of entries array */
  hash_cell   **entries;
} hash_map;

typedef struct turtle_state
{ wchar_t      *base_uri;		/* Base URI for <> */
  size_t	base_uri_len;		/* Length of base uri */
  size_t	base_uri_base_len;	/* Length upto last / */
  size_t	base_uri_host_len;	/* Length upto start of path */
  wchar_t      *empty_prefix;		/* Empty :local */
  hash_map	prefix_map;		/* Prefix --> IRI */
  hash_map	blank_node_map;		/* Name --> resource */
  size_t	bnode_id;		/* Blank node identifiers */
  struct
  { wchar_t *prefix;
    wchar_t *buffer;
    wchar_t *prefix_end;
  } bnode;
  resource     *current_subject;
  resource     *current_predicate;
  resource     *current_graph;
  resource     *default_graph;
  resource     *free_resources;		/* Recycle resources */
  IOSTREAM     *input;			/* Our input */
  int		current_char;		/* Current character */
  recover_mode	recover;		/* R_*: how to recover */
  recover_mode  recovered;		/* R_*: how we recovered */
  error_mode	on_error;		/* E_* */
  format	format;			/* D_* */
  size_t	error_count;		/* Number of syntax errors */
  size_t	count;			/* Counted triples */
  term_t	head;			/* Head of triple list */
  term_t	tail;			/* Tail of triple list */
} turtle_state;


typedef enum
{ NUM_INTEGER,
  NUM_DECIMAL,
  NUM_DOUBLE
} number_type;

#define RES_CONST(uri) { R_RESOURCE, TRUE, {{ uri, 0, {0} }} }

#define	RDF_TYPE_INDEX	  0
#define	RDF_FIRST_INDEX	  1
#define	RDF_REST_INDEX	  2
#define	RDF_NIL_INDEX	  3
#define	XSD_INTEGER_INDEX 4
#define	XSD_DECIMAL_INDEX 5
#define	XSD_DOUBLE_INDEX  6
#define	XSD_BOOLEAN_INDEX 7

#define RESOURCE(name) (&resource_constants[name ## _INDEX])

static resource resource_constants[] =
{ RES_CONST(RDF_TYPE),		/* 0 */
  RES_CONST(RDF_FIRST),		/* 1 */
  RES_CONST(RDF_REST),		/* 2 */
  RES_CONST(RDF_NIL),		/* 3 */
  RES_CONST(XSD_INTEGER),	/* 4 */
  RES_CONST(XSD_DECIMAL),	/* 5 */
  RES_CONST(XSD_DOUBLE),	/* 6 */
  RES_CONST(XSD_BOOLEAN)	/* 7 */
};

#define RECOVER_BEGIN(mode) { recover_mode __or = ts->recover; \
			      ts->recover = mode; \
			      ts->recovered = R_STATEMENT;
#define RECOVER_END()         ts->recover = __or; \
			    }
#define RECOVER(mode, code) \
        RECOVER_BEGIN(mode); \
	code; \
	RECOVER_END();


		 /*******************************
		 *	       BUFFER		*
		 *******************************/

#ifdef STRING_BUF_DEBUG
#define FAST_BUF_SIZE 1
#else
#define FAST_BUF_SIZE 512
#endif

typedef struct string_buffer
{ wchar_t fast[FAST_BUF_SIZE];
  wchar_t *buf;
  wchar_t *in;
  wchar_t *end;
#ifdef STRING_BUF_DEBUG
  int	   discarded;
#endif
} string_buffer;


static int
growBuffer(string_buffer *b, int c)
{ assert(c != -1);

  if ( b->buf == b->fast )
  { wchar_t *new = malloc((FAST_BUF_SIZE*2)*sizeof(wchar_t));

    if ( new )
    { memcpy(new, b->fast, sizeof(b->fast));
      b->buf = new;
      b->in  = b->buf+FAST_BUF_SIZE;
      b->end = b->in+FAST_BUF_SIZE;
      *b->in++ = c;

      return TRUE;
    }
  } else
  { size_t sz = b->end - b->buf;
    wchar_t *new = realloc(b->buf, sz*sizeof(wchar_t)*2);

    if ( new )
    { b->buf = new;
      b->in  = new+sz;
      b->end = b->in+sz;
      *b->in++ = c;

      return TRUE;
    }
  }

  return PL_resource_error("memory");
}


static inline void
initBuf(string_buffer *b)
{ b->buf = b->fast;
  b->in  = b->buf;
  b->end = &b->fast[FAST_BUF_SIZE];
#ifdef STRING_BUF_DEBUG
  b->discarded = FALSE;
#endif
}


static inline void
discardBuf(string_buffer *b)
{
#ifdef STRING_BUF_DEBUG
  assert(b->discarded == FALSE);
  b->discarded = TRUE;
#endif
  if ( b->buf != b->fast )
    free(b->buf);
}


static inline int
addBuf(string_buffer *b, int c)
{ if ( b->in < b->end )
  { *b->in++ = c;
    return TRUE;
  }

  return growBuffer(b, c);
}


static inline int
bufSize(string_buffer *b)
{ return b->in - b->buf;
}

#define baseBuf(b)   ( (b)->buf )


		 /*******************************
		 *	CHARACTER CLASSES	*
		 *******************************/

#define EOS ((wchar_t)(0))

#define WS	0x01			/* white space */
#define EL	0x02			/* end-of-line */
#define DI	0x04			/* digit */
#define LC	0x08			/* digit */
#define UC	0x10			/* digit */
#define IV	0x20			/* invalid */
#define EF	0x80			/* END-OF-FILE */
#define NI	0x100			/* Not IRI */
#define EC	0x200			/* Local escape */

static const short char_type0[] =
{   /*0      1      2      3      4      5      6      7
      8      9      A      B      C      D      E      F  */
  NI|EF,
     NI,    NI,    NI,    NI,    NI,    NI,    NI,    NI,   /* 00-07 */
     NI, WS|NI, EL|NI,	  NI,	 NI, EL|NI,    NI,    NI,   /* 08-0f */
     NI,    NI,    NI,    NI,    NI,    NI,    NI,    NI,   /* 10-17 */
     NI,    NI,    NI,    NI,    NI,    NI,    NI,    NI,   /* 18-1F */
  NI|WS,    EC,    NI,	  EC,	 EC,	EC,    EC,    EC,   /* 20-27 */
     EC,    EC,	   EC,	  EC,	 EC,	EC,    EC,    EC,   /* 28-2F */
     DI,    DI,    DI,    DI,    DI,    DI,    DI,    DI,   /* 30-37 */
     DI,    DI,     0,	  EC,    NI,	EC,    NI,    EC,   /* 38-3F */
     EC,    UC,    UC,    UC,    UC,    UC,    UC,    UC,   /* 40-47 */
     UC,    UC,    UC,    UC,    UC,    UC,    UC,    UC,   /* 48-4F */
     UC,    UC,    UC,    UC,    UC,    UC,    UC,    UC,   /* 50-57 */
     UC,    UC,    UC,     0,    NI,     0,    NI,    EC,   /* 58-5F */
     NI,    LC,    LC,    LC,    LC,    LC,    LC,    LC,   /* 60-67 */
     LC,    LC,    LC,    LC,    LC,    LC,    LC,    LC,   /* 68-6F */
     LC,    LC,    LC,    LC,    LC,    LC,    LC,    LC,   /* 70-77 */
     LC,    LC,    LC,    NI,    NI,    NI,    EC,     0    /* 78-7F */
};

static const short* char_type = &char_type0[1];

static inline int
is_ws(int c)
{ return (c < 128 ? (char_type[c] & (WS|EL)) != 0 : FALSE);
}

static inline int
is_eol(int c)
{ return (c < 128 ? (char_type[c] & EL) != 0 : FALSE);
}

static inline int
is_digit(int c)
{ return (c < 128 ? (char_type[c] & DI) != 0 : FALSE);
}

static inline int
is_scheme_char(int c)
{ return (c < 128 ? (char_type[c] & (LC|UC)) != 0 : FALSE);
}

static inline int
is_lang_char(int c, int minus)
{ if ( c < 128 )
  { if ( minus == 0 )			/* main language */
      return (char_type[c] & (LC|UC));
    else				/* sub language */
      return (char_type[c] & (LC|UC|DI));
  }

  return FALSE;
}

static inline int
is_local_escape(int c)
{ return (c < 128 ? (char_type[c] & EC) != 0 : FALSE);
}

static inline int
is_iri_char(int c)
{ return (c < 128 ? (char_type[c] & NI) == 0 : TRUE);
}

static const char hexval0[] =
{/*0   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F  */
  -1,
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 00-0f */
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 10-1F */
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 20-2F */
   0,  1,  2,  3,  4,  5,  6,  7,  8,  9, -1, -1, -1, -1, -1, -1, /* 30-3F */
  -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 40-4F */
  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 50-5F */
  -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1  /* 60-6F */
};

static const char* hexval = &hexval0[1];

static inline int
hexd(int c)
{ return (c <= 'f' ? hexval[c] : -1);
}



		 /*******************************
		 *	      HASHES		*
		 *******************************/

#define INIT_PREFIX_SIZE	64
#define INIT_BNODE_SIZE		64

static int
init_hash_map(hash_map *hm, int size)
{ hm->entries = malloc(sizeof(*hm->entries)*size);

  if ( hm->entries )
  { memset(hm->entries, 0, sizeof(*hm->entries)*size);
    hm->count = 0;
    hm->size  = size;

    return TRUE;
  }

  return FALSE;
}


static void
free_hash_cell(hash_cell *c)
{ if ( c->name )    free(c->name);
  if ( c->value.s ) free(c->value.s);

  free(c);
}


static void
clear_hash_table(hash_map *hm)
{ size_t i;

  for(i=0; i<hm->size; i++)		/* works for uninitialized map */
  { hash_cell *c, *n;

    for(c=hm->entries[i]; c; c=n)
    { n = c->next;
      free_hash_cell(c);
    }
  }

  free(hm->entries);
}


static int
wcs_hash(const wchar_t *name)
{ size_t len = wcslen(name);

  return rdf_murmer_hash(name, len*sizeof(wchar_t), MURMUR_SEED);
}


hash_cell *
lookup_hash_map(hash_map *hm, const wchar_t *name)
{ int key = wcs_hash(name) % hm->size;
  hash_cell *c;

  for(c = hm->entries[key]; c; c=c->next)
  { if ( wcscmp(name, c->name) == 0 )
      return c;
  }

  return NULL;
}


static int
add_hash_map(hash_map *hm, hash_cell *c)
{ int key = wcs_hash(c->name) % hm->size;

  c->next = hm->entries[key];
  hm->entries[key] = c;

  return TRUE;
}


static int
add_string_hash_map(hash_map *hm,
		    const wchar_t *name, const wchar_t *value)
{ hash_cell *c;

  if ( (c=malloc(sizeof(*c))) )
  { c->name    = wcsdup(name);
    c->value.s = wcsdup(value);

    return add_hash_map(hm, c);
  }

  return PL_resource_error("memory");
}






		 /*******************************
		 *	      ERROR		*
		 *******************************/

static int	next(turtle_state *ts);

static int
print_message(turtle_state *ts, term_t ex1, int is_error)
{ term_t ex;
  IOPOS *pos;

  if ( PL_exception(0) )		/* pending earlier exception */
    return FALSE;

  ts->error_count++;
  if ( !(ex=PL_new_term_refs(2)) ||
       !PL_put_term(ex+0, ex1) )
    return FALSE;

					/* create position term */
  if ( (pos=ts->input->position) )
  { term_t stream;
    int linepos = pos->linepos;
    int64_t charno = pos->charno;

    if ( linepos > 0 )
    { linepos--;
      charno--;
    }

    if ( !(stream = PL_new_term_ref()) ||
	 !PL_unify_stream(stream, ts->input) ||
	 !PL_unify_term(ex+1,
			PL_FUNCTOR, FUNCTOR_stream4,
			  PL_TERM, stream,
			  PL_INT, (int)pos->lineno,
			  PL_INT, linepos,
			  PL_INT64, charno) )
      return FALSE;
  }

					/* create error(Format, Pos) */
  if ( !PL_cons_functor_v(ex, FUNCTOR_error2, ex) )
    return FALSE;

  if ( is_error )			/* re-sync after error */
  { for(;;)
    { if ( !next(ts) || ts->current_char == -1 )
	break;
      if ( ts->current_char == '.' )
      { if ( !next(ts) ||
	     ts->current_char == -1 ||
	     is_ws(ts->current_char) )
	{ ts->recovered = R_STATEMENT;
	  break;
	}
      }
      if ( ( (ts->current_char == ';' && (ts->recover == R_PREDOBJLIST ||
					  ts->recover == R_OBJLIST) ) ||
	     (ts->current_char == ',' && ts->recover == R_OBJLIST)
	   ) && ts->on_error == E_WARNING )
      { ts->recovered = (ts->current_char == ';' ? R_PREDOBJLIST : R_OBJLIST);
	break;
      }
    }
  }

  if ( is_error && ts->on_error == E_ERROR )
  { return PL_raise_exception(ex);	/* raise exception */
  } else
  { static predicate_t print_message2;	/* or print message */
    term_t av;

    if ( !print_message2 )
      print_message2 = PL_predicate("print_message", 2, "system");

    if ( (av = PL_new_term_refs(2)) &&
	 PL_put_atom(av+0, is_error ? ATOM_error : ATOM_warning) &&
	 PL_put_term(av+1, ex) )
      PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, print_message2, av);

    return FALSE;
  }
}


static int
syntax_message(turtle_state *ts, const char *msg, int is_error)
{ term_t ex1;

  if ( PL_exception(0) )		/* pending earlier exception */
    return FALSE;

  ts->error_count++;

  if ( !(ex1=PL_new_term_ref()) ||
       !PL_unify_term(ex1, PL_FUNCTOR, FUNCTOR_syntax_error1,
		              PL_CHARS, msg) )
    return FALSE;

  return print_message(ts, ex1, is_error);
}

static int
syntax_warning(turtle_state *ts, const char *msg)
{ return syntax_message(ts, msg, FALSE);
}

static int
syntax_error(turtle_state *ts, const char *msg)
{ return syntax_message(ts, msg, TRUE);
}


		 /*******************************
		 *     CANONICAL RESOURCE	*
		 *******************************/

#define MAX_SAVEP 100

#define EOP(c) ((c) == 0 || (c) == '#' || (c) == '?')

static void
cpAfterPath(wchar_t *out, wchar_t *in)
{ while (*in )
    *out++ = *in++;
  *out = EOS;
}


static wchar_t *
url_skip_to_path(const wchar_t *in)
{ while( *in && *in != ':' )		/* skip scheme */
    in++;
  if ( *in == ':' && in[1] == '/' && in[2] == '/' )
    in += 3;
  while( *in && *in != '/' )		/* skip authority */
    in++;

  return (wchar_t *)in;
}


static wchar_t *
canonicaliseResourcePath(wchar_t *path)
{ wchar_t *in, *out, *start;
  wchar_t *save_buf[MAX_SAVEP];
  wchar_t **savep = save_buf;

  in = url_skip_to_path(path);

  if ( !in[0] )
    return path;
  out = start = in;			/* start of path */

  while( in[0] == '/' && in[1] == '.' && in[2] == '.' && in[3] == '/' )
    in += 3;
  while( in[0] == '.' && in[1] == '/' )
    in += 2;
  if ( in[0] == '/' )
    *out++ = '/';

  while(*in)
  { if (*in == '/')
    {
    again:
      if ( *in )
      { while( in[1] == '/' )		/* delete multiple / */
	  in++;
	if ( in[1] == '.' )
	{ if ( in[2] == '/' )		/* delete /./ */
	  { in += 2;
	    goto again;
	  }
	  if ( EOP(in[2]) )		/* delete trailing /. */
	  { cpAfterPath(out, in+2);
	    return path;
	  }
	  if ( in[2] == '.' && (in[3] == '/' || EOP(in[3])) )
	  { if ( savep > save_buf )	/* delete /foo/../ */
	    { out = *(--savep);
	      in += 3;
	      if ( EOP(in[0]) && out > start+1 )
	      { cpAfterPath(out-1, in);	/* delete trailing / */
		return path;
	      }
	      goto again;
	    } else if (	start[0] == '/' && out == start+1 )
	    { in += 3;
	      goto again;
	    }
	  }
	}
      }
      if ( *in )
	in++;
      if ( out > path && out[-1] != '/' )
	*out++ = '/';
      if ( savep < &save_buf[MAX_SAVEP-1] )
	*savep++ = out;
    } else
      *out++ = *in++;
  }
  *out++ = *in++;

  return path;
}


		 /*******************************
		 *	     SKIPPING		*
		 *******************************/

static int
skip_ws_no_comment(turtle_state *ts)
{ int c = ts->current_char;

  while(is_ws(c))
    c = Sgetcode(ts->input);

  ts->current_char = c;

  return !Sferror(ts->input);
}


static int
skip_comment_line(turtle_state *ts)
{ int c;

  do
  { c = Sgetcode(ts->input);
  } while ( c != -1 && !is_eol(c) );

  while(is_eol(c))
    c = Sgetcode(ts->input);

  ts->current_char = c;

  return !Sferror(ts->input);
}


/* skip_ws() skips white zero or more white space and #-comments
*/

static int
skip_ws(turtle_state *ts)
{ for(;;)
  { if ( skip_ws_no_comment(ts) )
    { if ( ts->current_char == '#' )
      { if ( skip_comment_line(ts) )
	  continue;
	return FALSE;
      }

      return TRUE;
    }

    return FALSE;
  }
}

static int
next(turtle_state *ts)
{ ts->current_char = Sgetcode(ts->input);

  return !Sferror(ts->input);
}


		 /*******************************
		 *	     RESOURCES		*
		 *******************************/

static void	free_resource(turtle_state *ts, resource *r);

static resource *
alloc_resource(turtle_state *ts)
{ resource *r;

  if ( (r=ts->free_resources) )
  { ts->free_resources = r->v.next;
    return r;
  }

  if ( (r=malloc(sizeof(*r))) )
    r->constant = FALSE;
  else
    PL_resource_error("memory");

  return r;
}


static resource *
set_uri_resource(turtle_state *ts, resource *r, const wchar_t *iri)
{ size_t len = wcslen(iri);

  r->type = R_RESOURCE;
  r->v.r.handle = 0;

  if ( len < FAST_URI )
  { wcscpy(r->v.r.fast, iri);
    r->v.r.name = r->v.r.fast;
  } else
  { if ( !(r->v.r.name = wcsdup(iri)) )
    { free_resource(ts, r);
      PL_resource_error("memory");
      return NULL;
    }
  }

  return r;
}


static resource *
new_resource(turtle_state *ts, const wchar_t *iri)
{ resource *r;

  if ( (r=alloc_resource(ts)) )
    return set_uri_resource(ts, r, iri);

  PL_resource_error("memory");
  return NULL;
}


static int
set_atom_resource(resource *r, atom_t handle)
{ if ( r->v.r.handle )
  { if ( r->v.r.handle == handle )
      return TRUE;
    PL_unregister_atom(r->v.r.handle);
  }
  r->v.r.handle = handle;

  return TRUE;
}


static resource *
atom_resource(turtle_state *ts, atom_t handle)
{ resource *r;

  if ( (r=alloc_resource(ts)) )
  { PL_register_atom(handle);
    r->v.r.handle = handle;
    r->v.r.name = NULL;

    return r;
  }

  return NULL;
}


static resource *
new_bnode_from_id(turtle_state *ts, size_t id)
{ resource *r;

  if ( (r=alloc_resource(ts)) )
  { r->type = R_BNODE;
    r->v.bnode_id = id;

    return r;
  }

  PL_resource_error("memory");
  return NULL;
}


static resource *
new_bnode(turtle_state *ts)
{ return new_bnode_from_id(ts, ++ts->bnode_id);
}


static void
clear_handle_resource(resource *r)
{ if ( !r->constant && r->v.r.handle )
    PL_unregister_atom(r->v.r.handle);
}

static void
clear_resource(resource *r)
{ if ( !r->constant && r->type == R_RESOURCE )
  { if ( r->v.r.name && r->v.r.name != r->v.r.fast )
      free(r->v.r.name);
    clear_handle_resource(r);
  }
}

static void
free_resource(turtle_state *ts, resource *r)
{ if ( !r->constant )
  { clear_resource(r);
    r->v.next = ts->free_resources;
    ts->free_resources = r;
  }
}

static void
free_resources(turtle_state *ts)
{ resource *r, *n;

  for(r=ts->free_resources; r; r=n)
  { n = r->v.next;
    free(r);
  }
}


static int
turtle_existence_error(turtle_state *ts, const char *what, term_t t)
{ term_t ex1;

  if ( (ex1 = PL_new_term_ref()) &&
       PL_unify_term(ex1,
		     PL_FUNCTOR, FUNCTOR_existence_error2,
		       PL_CHARS, what,
		       PL_TERM, t) )
  { return print_message(ts, ex1, TRUE);
  }

  return FALSE;
}


static resource *
resolve_iri(turtle_state *ts, wchar_t *prefix, wchar_t *local)
{ const wchar_t *prefix_iri;

  if ( prefix )
  { hash_cell *c;

    if ( (c=lookup_hash_map(&ts->prefix_map, prefix)) )
    { prefix_iri = c->value.s;
    } else
    { term_t t = PL_new_term_ref();

      if ( PL_unify_wchars(t, PL_ATOM, (size_t)-1, prefix) )
	turtle_existence_error(ts, "turtle_prefix", t);
      return FALSE;
    }
  } else if ( ts->empty_prefix )
  { prefix_iri = ts->empty_prefix;
  } else
  { term_t t = PL_new_term_ref();

    if ( PL_unify_wchars(t, PL_ATOM, 0, L"") )
      turtle_existence_error(ts, "turtle_prefix", t);
    return FALSE;
  }

  if ( local )
  { size_t plen = wcslen(prefix_iri);
    size_t llen = wcslen(local);
    resource *r;

    if ( (r=alloc_resource(ts)) )
    { wchar_t *name;

      if ( plen+llen < FAST_URI )
      { name = r->v.r.fast;
      } else
      { if ( !(name = malloc((plen+llen+1)*sizeof(wchar_t))) )
	{ free_resource(ts, r);
	  PL_resource_error("memory");
	  return NULL;
	}
      }

      wcscpy(name, prefix_iri);
      wcscpy(name+plen, local);
      r->type = R_RESOURCE;
      r->v.r.name = name;
      r->v.r.handle = 0;

      return r;
    }

    return NULL;
  } else
  { return new_resource(ts, prefix_iri);
  }
}


static resource *
make_absolute_resource(turtle_state *ts, const wchar_t *uri)
{ const wchar_t *s;
  size_t len, plen;
  resource *r;

  if ( !uri[0] )			/* <> */
    return new_resource(ts, ts->base_uri);

  if ( is_scheme_char(uri[0]) )		/* absolute uri */
  { for(s=&uri[1]; *s && is_scheme_char(*s); s++)
      ;
    if ( *s == ':' )
      return new_resource(ts, uri);
  }

  len = wcslen(uri);

  if ( uri[0] == '#' )			/* relative to file */
    plen = ts->base_uri_len;
  else if ( uri[0] == '/' )		/* relative to host */
    plen = ts->base_uri_host_len;
  else
    plen = ts->base_uri_base_len;

  if ( (r=alloc_resource(ts)) )
  { wchar_t *name;

    if ( plen+len < FAST_URI )
    { name = r->v.r.fast;
    } else
    { if ( !(name = malloc((plen+len+1)*sizeof(wchar_t))) )
      { free_resource(ts, r);
	PL_resource_error("memory");
	return NULL;
      }
    }

    wcsncpy(name, ts->base_uri, plen);
    wcscpy(name+plen, uri);
    canonicaliseResourcePath(name);

    r->type = R_RESOURCE;
    r->v.r.name = name;
    r->v.r.handle = 0;

    return r;
  }

  return NULL;
}


		 /*******************************
		 *	   PARSER OBJECT	*
		 *******************************/

static void	clear_turtle_parser(turtle_state *ts);
static int	init_base_uri(turtle_state *ts);
static int	read_predicate_object_list(turtle_state *ts, const char *end);
static int	read_object(turtle_state *ts);
static int	set_subject(turtle_state *ts, resource *r, resource **old);
static int	set_predicate(turtle_state *ts, resource *r, resource **old);
static int	set_graph(turtle_state *ts, resource *r, resource **old);
static int	set_default_graph(turtle_state *ts, resource *r, resource **old);
static int	statement(turtle_state *ts);

static turtle_state *
new_turtle_parser(IOSTREAM *s)
{ turtle_state *ts = malloc(sizeof(*ts));

  if ( ts )
  { memset(ts, 0, sizeof(*ts));

    ts->input	 = s;
    if ( init_hash_map(&ts->prefix_map, INIT_PREFIX_SIZE) &&
	 next(ts) )			/* read first character */
    { return ts;
    }

    clear_turtle_parser(ts);
  }

  PL_resource_error("memory");
  return NULL;
}


static void
clear_turtle_parser(turtle_state *ts)
{ if ( ts->base_uri )	     free(ts->base_uri);
  if ( ts->empty_prefix )    free(ts->empty_prefix);
  if ( ts->bnode.buffer )    free(ts->bnode.buffer);

  if ( ts->input ) PL_release_stream(ts->input);

  set_subject(ts, NULL, NULL);
  set_predicate(ts, NULL, NULL);
  set_graph(ts, NULL, NULL);
  set_default_graph(ts, NULL, NULL);

  free_resources(ts);

  clear_hash_table(&ts->prefix_map);
  clear_hash_table(&ts->blank_node_map);

  memset(ts, 0, sizeof(*ts));			/* second call does nothing */
}


static void
free_turtle_parser(turtle_state *ts)
{ clear_turtle_parser(ts);

  free(ts);
}


static int
set_subject(turtle_state *ts, resource *r, resource **old)
{ if ( old )
  { *old = ts->current_subject;
  } else
  { if ( ts->current_subject )
      free_resource(ts, ts->current_subject);
  }
  ts->current_subject = r;

  return TRUE;
}


static int
set_anon_subject(turtle_state *ts, resource **old)
{ resource *r;

  if ( (r=new_bnode(ts)) )
  { set_subject(ts, r, old);
    return TRUE;
  }

  return FALSE;
}


static int
set_predicate(turtle_state *ts, resource *r, resource **old)
{ if ( old )
  { *old = ts->current_predicate;
  } else
  { if ( ts->current_predicate )
      free_resource(ts, ts->current_predicate);
  }
  ts->current_predicate = r;

  return TRUE;
}


static int
set_format(turtle_state *ts, format fmt)
{ if ( ts->format != fmt )
  { switch(fmt)
    { case D_TRIG:
	ts->default_graph = ts->current_graph;
        ts->current_graph = NULL;
	/*FALLTHROUGH*/
      case D_TRIG_NO_GRAPH:
      case D_TURTLE:
	ts->format = fmt;
	return TRUE;
      default:
	assert(0);
        return FALSE;
    }
  }

  return TRUE;
}


static int
set_graph(turtle_state *ts, resource *r, resource **old)
{ if ( old )
  { *old = ts->current_graph;
  } else
  { if ( ts->current_graph && ts->current_graph != ts->default_graph )
      free_resource(ts, ts->current_graph);
  }
  ts->current_graph = r;

  return TRUE;
}


static int
set_default_graph(turtle_state *ts, resource *r, resource **old)
{ if ( old )
  { *old = ts->default_graph;
  } else
  { if ( ts->default_graph )
      free_resource(ts, ts->default_graph);
  }
  ts->default_graph = r;

  return TRUE;
}


static int
set_empty_prefix(turtle_state *ts, const resource *r)
{ wchar_t *d;

  assert(r->type == R_RESOURCE);

  if ( (d=wcsdup(r->v.r.name)) )
  { if ( ts->empty_prefix )
      free(ts->empty_prefix);
    ts->empty_prefix = d;

    return TRUE;
  }

  return PL_resource_error("memory");
}


static int
init_base_uri(turtle_state *ts)
{ const wchar_t *s;

  ts->base_uri_len = wcslen(ts->base_uri);
  for(s = &ts->base_uri[ts->base_uri_len];
      s > ts->base_uri && s[-1] != '/';
      s--)
    ;
  ts->base_uri_base_len = s-ts->base_uri;
  ts->base_uri_host_len = url_skip_to_path(ts->base_uri)-ts->base_uri;

  return TRUE;
}


static int
set_base_uri(turtle_state *ts, const resource *r)
{ wchar_t *old = ts->base_uri;

  assert(r->type == R_RESOURCE);

  if ( (ts->base_uri = wcsdup(r->v.r.name)) )
  { if ( old )
      free(old);

    return init_base_uri(ts);
  }

  return PL_resource_error("memory");
}


static int
set_prefix(turtle_state *ts, const wchar_t *prefix, resource *r)
{ hash_cell *c;

  assert(r->type == R_RESOURCE);

  if ( (c=lookup_hash_map(&ts->prefix_map, prefix)) )
  { wchar_t *d;

    if ( (d=wcsdup(r->v.r.name)) )
    { if ( c->value.s )
	free(c->value.s);
      c->value.s = d;

      return TRUE;
    }

    return PL_resource_error("memory");
  } else
  { return add_string_hash_map(&ts->prefix_map, prefix, r->v.r.name);
  }
}


static char *
r_name(resource *r, char *buf, size_t size)
{ if ( r->type == R_RESOURCE )
    Ssnprintf(buf, size, "<%Ws>", r->v.r.name);
  else if ( r->type == R_BNODE )
    Ssnprintf(buf, size, "bnode(%ld)", (long)r->v.bnode_id);
  else
    assert(0);

  return buf;
}


static char *
o_name(object *o, char *buf, size_t size)
{ if ( o->type == O_RESOURCE )
    return r_name(o->value.r, buf, size);
  if ( o->type == O_LITERAL )
  { if ( o->value.l.lang )
      Ssnprintf(buf, size, "\"%Ws\"@%Ws", o->value.l.string, o->value.l.lang);
    else if ( o->value.l.type )
      Ssnprintf(buf, size, "\"%Ws\"^^<%Ws>", o->value.l.string,
	       o->value.l.type->v.r);
    else
      Ssnprintf(buf, size, "\"%Ws\"", o->value.l.string);

    return buf;
  }
  assert(0);
  return NULL;
}


static int
put_resource(turtle_state *ts, term_t t, resource *r)
{ switch ( r->type )
  { case R_RESOURCE:
    { if ( !r->v.r.handle )
	r->v.r.handle = PL_new_atom_wchars(wcslen(r->v.r.name), r->v.r.name);
      return PL_put_atom(t, r->v.r.handle);
    }
    case R_BNODE:
    { if ( ts->bnode.prefix )
      { if ( !ts->bnode.buffer )
	{ size_t plen = wcslen(ts->bnode.prefix);

	  ts->bnode.buffer = malloc((plen+64)*sizeof(wchar_t));
	  if ( !ts->bnode.buffer )
	    return PL_resource_error("memory");
	  wcscpy(ts->bnode.buffer, ts->bnode.prefix);
	  ts->bnode.prefix_end = &ts->bnode.buffer[plen];
	}
	swprintf(ts->bnode.prefix_end, 64, L"%ld", (long)r->v.bnode_id);
	PL_put_variable(t);
	return PL_unify_wchars(t, PL_ATOM, (size_t)-1, ts->bnode.buffer);
      } else
      { return ( PL_put_int64(t, r->v.bnode_id) &&
		 PL_cons_functor_v(t, FUNCTOR_node1, t)
	       );
      }
    }
  }


  return FALSE;
}


static int
put_object(turtle_state *ts, term_t t, object *o)
{ if ( o->type == O_RESOURCE )
  { return put_resource(ts, t, o->value.r);
  } else
  { if ( o->value.l.lang )
    { term_t av = PL_new_term_refs(2);

      if ( !PL_unify_wchars(av+0, PL_ATOM, (size_t)-1, o->value.l.lang) ||
	   !PL_unify_wchars(av+1, PL_ATOM, o->value.l.len, o->value.l.string) ||
	   !PL_cons_functor_v(t, FUNCTOR_lang2, av) )
	return FALSE;
    } else if ( o->value.l.type )
    { term_t av = PL_new_term_refs(2);

      if ( !put_resource(ts, av+0, o->value.l.type) ||
	   !PL_unify_wchars(av+1, PL_ATOM, o->value.l.len, o->value.l.string) ||
	   !PL_cons_functor_v(t, FUNCTOR_type2, av) )
	return FALSE;
    } else
    { PL_put_variable(t);
      if ( !PL_unify_wchars(t, PL_ATOM, o->value.l.len, o->value.l.string) )
	return FALSE;
    }

    return PL_cons_functor_v(t, FUNCTOR_literal1, t);
  }
}


static int
put_graph(turtle_state *ts, term_t g)
{ resource *cg;

  if ( (cg=ts->current_graph) )
  { IOPOS *pos;

    if ( !cg->v.r.handle )
    { cg->v.r.handle = PL_new_atom_wchars(wcslen(cg->v.r.name), cg->v.r.name);
    }

    if ( (pos=ts->input->position) )
    { PL_put_variable(g);
      return PL_unify_term(g, PL_FUNCTOR, FUNCTOR_colon2,
			        PL_ATOM, cg->v.r.handle,
			        PL_INT,  (int)pos->lineno);
    } else
    { return PL_put_atom(g, cg->v.r.handle);
    }
  } else
  { return TRUE;
  }
}


static int
got_triple(turtle_state *ts, resource *s, resource *p, object *o)
{ if ( ts->count++ == 0 && ts->format == D_AUTO )
    set_format(ts, D_TURTLE);

  if ( ts->tail )
  { term_t av = PL_new_term_refs(4);
    functor_t rdff = (ts->current_graph ? FUNCTOR_rdf4 : FUNCTOR_rdf3);

    if ( put_resource(ts, av+0, s) &&
	 put_resource(ts, av+1, p) &&
	 put_object(  ts, av+2, o) &&
	 put_graph(   ts, av+3)    &&
	 PL_cons_functor_v(av+0, rdff, av) &&
	 PL_unify_list(ts->tail, ts->head, ts->tail) &&
	 PL_unify(ts->head, av+0) )
    { PL_reset_term_refs(av+0);
      return TRUE;
    } else
    { return FALSE;
    }
  } else
  { char sbuf[256];
    char pbuf[256];
    char obuf[256];

    Sdprintf("Got %s %s %s\n",
	     r_name(s, sbuf, sizeof(sbuf)),
	     r_name(p, pbuf, sizeof(pbuf)),
	     o_name(o, obuf, sizeof(obuf)));

    return TRUE;
  }
}

static int
got_resource_triple(turtle_state *ts, resource *r)
{ object o;

  o.type = O_RESOURCE;
  o.value.r = r;

  return got_triple(ts, ts->current_subject, ts->current_predicate, &o);
}


static int
got_anon_triple(turtle_state *ts)
{ resource anon;

  anon.type = R_BNODE;
  anon.v.bnode_id = ++ts->bnode_id;

  return got_resource_triple(ts, &anon);
}


static int
got_next_triple(turtle_state *ts, resource *prev, resource *next)
{ object o;

  o.type = O_RESOURCE;
  o.value.r = next;

  return got_triple(ts, prev, RESOURCE(RDF_REST), &o);
}


static int
got_literal_triple(turtle_state *ts, object *o)
{ return got_triple(ts, ts->current_subject, ts->current_predicate, o);
}


static int
got_lang_triple(turtle_state *ts, size_t len, const wchar_t *text,
		const wchar_t *lang)
{ object o;

  o.type = O_LITERAL;
  o.value.l.len	   = len;
  o.value.l.string = (wchar_t*)text;
  o.value.l.lang   = (wchar_t*)lang;
  o.value.l.type   = NULL;

  return got_literal_triple(ts, &o);
}


static int
got_typed_triple(turtle_state *ts, size_t len, const wchar_t *text,
		 resource *type)
{ object o;

  o.type = O_LITERAL;
  o.value.l.len	   = len;
  o.value.l.string = (wchar_t*)text;
  o.value.l.lang   = NULL;
  o.value.l.type   = type;

  return got_literal_triple(ts, &o);
}

static int
got_plain_triple(turtle_state *ts, size_t len, const wchar_t *text)
{ object o;

  o.type = O_LITERAL;
  o.value.l.len	   = len;
  o.value.l.string = (wchar_t*)text;
  o.value.l.lang   = NULL;
  o.value.l.type   = NULL;

  return got_literal_triple(ts, &o);
}

static resource *
numeric_type(number_type type)
{ switch(type)
  { case NUM_INTEGER: return RESOURCE(XSD_INTEGER);
    case NUM_DECIMAL: return RESOURCE(XSD_DECIMAL);
    case NUM_DOUBLE:  return RESOURCE(XSD_DOUBLE);
    default:	      assert(0); return NULL;
  }
}

static int
got_numeric_triple(turtle_state *ts, const wchar_t *text, number_type type)
{ object o;

  o.type = O_LITERAL;
  o.value.l.len    = (size_t)-1;
  o.value.l.string = (wchar_t*)text;
  o.value.l.lang   = NULL;
  o.value.l.type   = numeric_type(type);

  return got_literal_triple(ts, &o);
}


static int
got_boolean_triple(turtle_state *ts, int istrue)
{ object o;

  o.type = O_LITERAL;
  o.value.l.len    = (size_t)-1;
  o.value.l.string = istrue ? L"true" : L"false";
  o.value.l.lang   = NULL;
  o.value.l.type   = RESOURCE(XSD_BOOLEAN);

  return got_literal_triple(ts, &o);
}



		 /*******************************
		 *	      PARSING		*
		 *******************************/

static inline int
wcis_pn_chars_u(int c)			/* 164s */
{ return ( wcis_pn_chars_base(c) ||
	   c == '_'
	 );
}


static inline int
wcis_pn_chars(int c)
{ return ( wcis_pn_chars_u(c) ||
	   wcis_pn_chars_extra(c)
	 );
}


static int
read_hex(turtle_state *ts, int count, int *cp)
{ int c = 0;

  while(count-->0)
  { if ( next(ts) )
    { int v;

      if ( (v=hexd(ts->current_char)) >= 0 )
	c = 16*c+v;
      else
	return syntax_error(ts, "Illegal UCHAR");
    } else
      return FALSE;
  }

  *cp = c;
  return TRUE;
}


static int
read_uchar(turtle_state *ts, int *cp)
{ if ( next(ts) )
  { switch(ts->current_char)
    { case 'u':
	return read_hex(ts, 4, cp);
      case 'U':
	return read_hex(ts, 8, cp);
      default:
	return syntax_error(ts, "Illegal \\-escape");
    }
  }

  return FALSE;
}


static inline int
starts_plx(int c)
{ return (c == '%' || c == '\\');
}

static int
read_plx(turtle_state *ts, string_buffer *b)
{ if ( ts->current_char == '%' )
  { int h1, h2;

    if ( next(ts) && hexd(h1=ts->current_char) >= 0 &&
	 next(ts) && hexd(h2=ts->current_char) >= 0 )
    { addBuf(b, '%');
      addBuf(b, h1);
      addBuf(b, h2);

      return TRUE;
    }

    return syntax_error(ts, "Illegal %XX escape");
  } else if ( ts->current_char == '\\' )
  { if ( next(ts) && is_local_escape(ts->current_char) )
    { addBuf(b, ts->current_char);

      return TRUE;
    }

    return syntax_error(ts, "Illegal \\-escape in local name");
  }

  return FALSE;
}


static resource *
read_iri_ref(turtle_state *ts)
{ string_buffer b;

  initBuf(&b);

  for(;;)
  { int c;

    if ( !next(ts) )
      return FALSE;
    c = ts->current_char;

    if ( is_iri_char(c) )
    { addBuf(&b, c);
    } else
    { switch(c)
      { case '>':
	{ resource *r;

	  next(ts);
	  addBuf(&b, EOS);
	  r = make_absolute_resource(ts, baseBuf(&b));
	  discardBuf(&b);
	  return r;
	}
	case '\\':
	{ if ( read_uchar(ts, &c) )
	  { addBuf(&b, c);
	  } else
	  { discardBuf(&b);
	    return NULL;
	  }
	  break;
	}
	default:
	{ discardBuf(&b);
	  syntax_error(ts, "Illegal IRIREF");
	  return NULL;
	}
      }
    }
  }
}


static int
read_pn_prefix(turtle_state *ts, string_buffer *b)
{ if ( wcis_pn_chars_base(ts->current_char) )
  { initBuf(b);
    addBuf(b, ts->current_char);
  } else
  { return syntax_error(ts, "PN_PREFIX expected");
  }

  for(;;)
  { if ( next(ts) )
    { if ( wcis_pn_chars(ts->current_char) )
      { addBuf(b, ts->current_char);
	continue;
      }
      if ( ts->current_char == '.' )
      { int c = Speekcode(ts->input);

	if ( wcis_pn_chars(c) || c == '.' )
	{ addBuf(b, ts->current_char);
	  continue;
	}
      }

      addBuf(b, EOS);
      return TRUE;
    } else
    { discardBuf(b);
      return FALSE;
    }
  }
}


static int
pn_local_start(int c)
{ return ( wcis_pn_chars_u(c) ||
	   c == ':' ||
	   is_digit(c) );
}


static int
read_pn_local(turtle_state *ts, string_buffer *b)
{ if ( pn_local_start(ts->current_char) )
  { initBuf(b);
    addBuf(b, ts->current_char);
  } else if ( starts_plx(ts->current_char) )
  { initBuf(b);
    if ( !read_plx(ts, b) )
    { discardBuf(b);
      return FALSE;
    }
  } else
  { return syntax_error(ts, "PN_LOCAL expected");
  }

  for(;;)
  { if ( next(ts) )
    { if ( wcis_pn_chars(ts->current_char) ||
	   ts->current_char == ':' )
      { addBuf(b, ts->current_char);
	continue;
      }
      if ( starts_plx(ts->current_char) )
      { if ( !read_plx(ts, b) )
	  return FALSE;
	continue;
      }
      if ( ts->current_char == '.' )
      { int c = Speekcode(ts->input);

	if ( wcis_pn_chars(c) || c == ':' || c == '.' || c == '%' )
	{ addBuf(b, ts->current_char);
	  continue;
	}
      }

      addBuf(b, EOS);
      return TRUE;
    } else
      return FALSE;
  }
}


static resource *
read_collection(turtle_state *ts)
{ resource *olds, *oldp, *collection = NULL;

  if ( !next(ts) || !skip_ws(ts) )
    return FALSE;

  for(;;)
  { if ( ts->current_char == ')' )
    { if ( !next(ts) )
	goto error_out;

      if ( collection )
      { resource *prev;
	int rc;

	set_subject(ts, olds, &prev);
	set_predicate(ts, oldp, NULL);
	rc = got_next_triple(ts, prev, RESOURCE(RDF_NIL));
	if ( !rc || prev != collection )
	  free_resource(ts, prev);

	return rc ? collection : NULL;
      } else
      { return RESOURCE(RDF_NIL);
      }
    }

    if ( collection )
    { resource *prev;

      if ( !set_anon_subject(ts, &prev) ||
	   !got_next_triple(ts, prev, ts->current_subject) )
	goto error_out;
      if ( prev != collection )
	free_resource(ts, prev);
    } else
    { if ( !set_anon_subject(ts, &olds) ||
	   !set_predicate(ts, RESOURCE(RDF_FIRST), &oldp) )
	return FALSE;
      collection = ts->current_subject;
    }

    if ( !read_object(ts) ||
	 !skip_ws(ts) )
    { error_out:
      if ( collection )
      { set_subject(ts, olds, NULL);		/* restore */
	set_predicate(ts, oldp, NULL);
      }
      return NULL;
    }
  }
}


/* process [ predicate_object_list ].  The first "[" has already
   been eaten.
*/

static resource *
read_blank_node_property_list(turtle_state *ts)
{ resource *olds, *bnode, *oldp = NULL;
  int rc;

  rc = ( set_anon_subject(ts, &olds) &&
	 set_predicate(ts, NULL, &oldp) &&
	 read_predicate_object_list(ts, "]")
       );
  set_subject(ts, olds, &bnode);
  set_predicate(ts, oldp, NULL);

  if ( rc && ts->current_char == ']' && next(ts) )
    return bnode;

  if ( rc )
    syntax_error(ts, "Expected \"]\"");

  return NULL;
}


static resource *
read_blank_node_label(turtle_state *ts)
{ if ( !next(ts) ) return FALSE;
  if ( ts->current_char != ':' )
  { syntax_error(ts, "Expected \":\" after \"_\"");
    return NULL;
  }
  if ( !next(ts) ) return FALSE;
  if ( wcis_pn_chars_u(ts->current_char) || is_digit(ts->current_char) )
  { string_buffer name;
    hash_cell *c;

    initBuf(&name);
    addBuf(&name, ts->current_char);
    for(;;)
    { if ( next(ts) )
      { if ( wcis_pn_chars(ts->current_char) )
	{ addBuf(&name, ts->current_char);
	  continue;
	}
	if ( ts->current_char == '.' )
	{ int c = Speekcode(ts->input);

	  if ( wcis_pn_chars(c) || c == '.' )
	  { addBuf(&name, ts->current_char);
	    continue;
	  }
	}

	addBuf(&name, EOS);
	break;
      } else
	return FALSE;
    }

    if ( !ts->blank_node_map.entries )	/* lazy creation of bnode map */
    { if ( !init_hash_map(&ts->blank_node_map, INIT_BNODE_SIZE) )
	return FALSE;
    }

    if ( (c=lookup_hash_map(&ts->blank_node_map, baseBuf(&name))) )
    { discardBuf(&name);
      return new_bnode_from_id(ts, c->value.bnode_id);
    } else
    { resource *r;

      if ( (r=new_bnode(ts)) )
      { if ( (c=malloc(sizeof(*c))) )
	{ memset(c, 0, sizeof(*c));

	  if ( (c->name = wcsdup(baseBuf(&name))) )
	  { c->value.bnode_id = r->v.bnode_id;
	    add_hash_map(&ts->blank_node_map, c);
	    discardBuf(&name);
	    return r;
	  }
	  free(c);
	}
	free_resource(ts, r);
      }
      discardBuf(&name);
      PL_resource_error("memory");
      return NULL;
    }
  } else
  { syntax_error(ts, "Blank node identifier expected");
    return NULL;
  }
}



#define IRI_VERB 0x1
#define IRI_BOOL 0x2

#define LITERAL_TRUE	((resource*)1)	/* literal true */
#define LITERAL_FALSE	((resource*)2)	/* literal false */


static resource *
read_iri(turtle_state *ts, int flags)
{ if ( !skip_ws(ts) )
    return FALSE;

  switch ( ts->current_char )
  { case '<':
    { return read_iri_ref(ts);
    }
    case ':':
    { if ( !next(ts) )
	return FALSE;

      if ( pn_local_start(ts->current_char) || starts_plx(ts->current_char) )
      { string_buffer pn_local;

	if ( read_pn_local(ts, &pn_local) )
	{ resource *r;
	  r = resolve_iri(ts, NULL, baseBuf(&pn_local));
	  discardBuf(&pn_local);
	  return r;
	}
      } else
      { return resolve_iri(ts, NULL, NULL);
      }

      return FALSE;
    }
    default:
    { string_buffer pn_prefix;

      if ( read_pn_prefix(ts, &pn_prefix) )
      { resource *r = NULL;

	if ( ts->current_char == ':' )
	{ if ( !next(ts) )
	  { r = NULL;
	  } else if ( pn_local_start(ts->current_char) ||
		      starts_plx(ts->current_char))
	  { string_buffer pn_local;

	    if ( read_pn_local(ts, &pn_local) )
	    { r = resolve_iri(ts, baseBuf(&pn_prefix), baseBuf(&pn_local));
	      discardBuf(&pn_local);
	    }
	  } else
	    r = resolve_iri(ts, baseBuf(&pn_prefix), NULL);
	} else if ( (flags&IRI_VERB) && wcscmp(baseBuf(&pn_prefix), L"a") == 0 )
	{ r = RESOURCE(RDF_TYPE);
	} else if ( (flags&IRI_BOOL) )
	{ if ( wcscmp(baseBuf(&pn_prefix), L"true") == 0 )
	    r = LITERAL_TRUE;
	  else if ( wcscmp(baseBuf(&pn_prefix), L"false") == 0 )
	    r = LITERAL_FALSE;
	}
	discardBuf(&pn_prefix);
	if ( !r )
	  syntax_error(ts, "Expected \":\"");
	return r;
      }

      return FALSE;
    }
  }
}


/* read_verb() reads a predicate.  If the predicate cannot be read, input
   is advanced to the next ; or end-of-statement.
*/

static int
read_verb(turtle_state *ts)
{ for(;;)
  { resource *r;

    RECOVER(R_PREDOBJLIST,
	    r=read_iri(ts, IRI_VERB));
    if ( r )
      return set_predicate(ts, r, NULL);
    if ( ts->recovered == R_PREDOBJLIST && next(ts) )
      continue;
    break;
  }

  return FALSE;
}


static int
read_echar_or_uchar(turtle_state *ts, int *cp)
{ if ( next(ts) )
  { switch(ts->current_char)
    { case 't':   *cp = '\t'; return TRUE;
      case 'b':   *cp = '\b'; return TRUE;
      case 'n':   *cp = '\n'; return TRUE;
      case 'r':   *cp = '\r'; return TRUE;
      case 'f':   *cp = '\f'; return TRUE;
      case '\\':  *cp = '\\'; return TRUE;
      case '"':   *cp = '\"'; return TRUE;
      case '\'':  *cp = '\''; return TRUE;
      case 'u':   return read_hex(ts, 4, cp);
      case 'U':   return read_hex(ts, 8, cp);
      default:
	return syntax_error(ts, "Illegal \\-escape in string");
    }
  }

  return FALSE;
}


static int
read_short_string(turtle_state *ts, int q, string_buffer *text)
{ do
  { switch(ts->current_char)
    { case '\n':
      case '\r':
	discardBuf(text);
	return syntax_error(ts, "Unexpected newline in short string");
      case -1:
	discardBuf(text);
	return syntax_error(ts, "End-of-file in short string");
      case '\\':
      { int c;

	if ( read_echar_or_uchar(ts, &c) )
	{ addBuf(text, c);
	  continue;
	}
	discardBuf(text);
	return FALSE;
      }
      default:
	if ( ts->current_char == q )
	{ addBuf(text, EOS);
	  return next(ts);
	} else
	{ addBuf(text, ts->current_char);
	}
    }
  } while(next(ts));

  discardBuf(text);
  return FALSE;
}


static int
read_long_string(turtle_state *ts, int q, string_buffer *text)
{ do
  { retry:
    switch(ts->current_char)
    { case '\\':
      { int c;

	if ( read_echar_or_uchar(ts, &c) )
	{ addBuf(text, c);
	  continue;
	}
	discardBuf(text);
	return FALSE;
      }
      case -1:
	discardBuf(text);
	return syntax_error(ts, "End-of-file in long string");
      default:
	if ( ts->current_char == q )		/* one */
	{ if ( !next(ts) ) return FALSE;
	  if ( ts->current_char == q )		/* two */
	  { if ( !next(ts) ) return FALSE;
	    if ( ts->current_char == q )	/* three */
	    { addBuf(text, EOS);
	      return next(ts);
	    } else
	    { addBuf(text, q);
	      addBuf(text, q);
	    }
	  } else
	  { addBuf(text, q);
	  }

	  goto retry;
	} else
	{ addBuf(text, ts->current_char);
	}
    }
  } while(next(ts));

  discardBuf(text);
  return FALSE;
}


static int
read_lang(turtle_state *ts, string_buffer *b)
{ int minus = 0;
  int fminus = TRUE;

  initBuf(b);

  for(;;)
  { if ( is_lang_char(ts->current_char, minus) )
    { addBuf(b, ts->current_char);
      fminus = FALSE;
    } else if ( ts->current_char == '-' && fminus == FALSE )
    { addBuf(b, ts->current_char);
      minus++;
      fminus = TRUE;
    } else if ( fminus == FALSE )
    { addBuf(b, EOS);
      return TRUE;
    } else
    { return syntax_error(ts, "LANGTAG expected");
    }

    if ( !next(ts) )
      return FALSE;
  }
}


/* read_string() reads a Turtle string.  Stops with the next
   character (after the string) cached.
*/

static int
read_string(turtle_state *ts, string_buffer *text)
{ int q = ts->current_char;

  if ( !next(ts) )
    return FALSE;
  initBuf(text);
  if ( ts->current_char == q )
  { if ( Speekcode(ts->input) == q )
    { next(ts);
      return next(ts) && read_long_string(ts, q, text);
    } else
    { addBuf(text, EOS);		/* empty '' or "" */
      return next(ts);
    }
  } else
  { return read_short_string(ts, q, text);
  }
}


static int
read_digits(turtle_state *ts, string_buffer *b)
{ int count = 0;

  for(;;)
  { if ( is_digit(ts->current_char) )
    { addBuf(b, ts->current_char);
      count++;
    } else
      return count;

    if ( !next(ts) )
    { discardBuf(b);
      return -1;
    }
  }
}


/* reading the exponent.  Note that this is always the last, and therefore
   we either close or discard the buffer
*/

static int
read_exponent(turtle_state *ts, string_buffer *num)
{ int rc;

  addBuf(num, ts->current_char);		/* add [eE] */
  if ( !next(ts) ) return FALSE;	/* skip [eE] */
  if ( (ts->current_char == '-' || ts->current_char == '+') &&
       is_digit(Speekcode(ts->input)) )
  { addBuf(num, ts->current_char);
    next(ts);
  }

  if ( (rc=read_digits(ts, num)) > 0 )
  { addBuf(num, EOS);
    return TRUE;
  }

  if ( rc == 0 )
    discardBuf(num);

  return FALSE;
}


static int
read_number(turtle_state *ts, string_buffer *num, number_type *numtype)
{ int rc;

  *numtype = NUM_INTEGER;
  initBuf(num);

  if ( ts->current_char == '-' || ts->current_char == '+' )
  { addBuf(num, ts->current_char);
    if ( !next(ts) )
    { discardBuf(num);
      return FALSE;
    }
  }
					/* +- */
  if ( (rc=read_digits(ts, num)) < 0 )
    return FALSE;
					/* +-[0-9]* */
  switch(ts->current_char)
  { case '.':
    { int c = Speekcode(ts->input);
      if ( !is_digit(c) && c != 'e' && c != 'E' )
      { if ( rc == 0 )			/* lone ., +., -. */
	  return FALSE;
	goto done;
      }

      *numtype = NUM_DECIMAL;
      addBuf(num, '.');
      if ( !next(ts) ) return FALSE;
      if ( (rc=read_digits(ts, num)) < 0 )
	return FALSE;
      switch(ts->current_char)
      { case 'e':
	case 'E':
	{ *numtype = NUM_DOUBLE;
	  return read_exponent(ts, num);
	}
        default:
        done:
	  addBuf(num, EOS);
	  return TRUE;
      }
    }
    case 'e':
    case 'E':
    { *numtype = NUM_DOUBLE;
      return read_exponent(ts, num);
    }
    default:
      addBuf(num, EOS);
      return TRUE;
  }
}


static int
read_object(turtle_state *ts)
{ if ( !skip_ws(ts) )
    return FALSE;

  switch(ts->current_char)
  { case '<':
    { resource *r;

      if ( (r=read_iri_ref(ts)) )
      { int rc = got_resource_triple(ts, r);
	free_resource(ts, r);
	return rc;
      }
    }
    case '[':
    { resource *r;

      if ( !(next(ts) && skip_ws(ts)) )
	return FALSE;
      if ( ts->current_char == ']' )
	return next(ts) && got_anon_triple(ts);
      else if ( (r=read_blank_node_property_list(ts)) )
      { int rc = got_resource_triple(ts, r);
	free_resource(ts, r);
	return rc;
      }
      return FALSE;
    }
    case '(':
    { resource *r;

      if ( (r=read_collection(ts)) )
      { int rc = got_resource_triple(ts, r);
	free_resource(ts, r);
	return rc;
      }
      return FALSE;
    }
    case '_':
    { resource *r;

      if ( (r=read_blank_node_label(ts)) )
      { int rc = got_resource_triple(ts, r);
	free_resource(ts, r);
	return rc;
      }

      return FALSE;
    }
    case '.':				/* Decimal */
    { int c = Speekcode(ts->input);
      if ( !is_digit(c) )
	return syntax_error(ts, "Unexpected \".\" (missing object)");
    }
    /*FALLTHROUGH*/
    case '+':
    case '-':				/* Signed INTEGER|DECIMAL|DOUBLE */
    { string_buffer num;
      number_type numtype;

    case_number:
      if ( read_number(ts, &num, &numtype) )
      { int rc;

	rc = got_numeric_triple(ts, baseBuf(&num), numtype);
	discardBuf(&num);

	return rc;
      }
    }
    case '"':
    case '\'':				/* string */
    { string_buffer text;

      if ( read_string(ts, &text) )
      { int rc;

	if ( !skip_ws(ts) )
	{ discardBuf(&text);
	  return FALSE;
	}

	if ( ts->current_char == '@' )
	{ string_buffer lang;

	  if ( (rc = next(ts) && skip_ws(ts) && read_lang(ts, &lang)) )
	  { rc = got_lang_triple(ts,
				 bufSize(&text)-1, baseBuf(&text),
				 baseBuf(&lang));
	    discardBuf(&lang);
	  }
	} else if ( ts->current_char == '^' )
	{ if ( next(ts) && ts->current_char == '^' )
	  { resource *r;

	    if ( next(ts) && skip_ws(ts) && (r=read_iri(ts, 0)) )
	    { rc = got_typed_triple(ts, bufSize(&text)-1, baseBuf(&text), r);
	      free_resource(ts, r);
	    } else
	    { rc = FALSE;
	    }
	  } else
	  { rc = syntax_error(ts, "Invalid literal, expected ^");
	  }
	} else
	{ rc = got_plain_triple(ts, bufSize(&text)-1, baseBuf(&text));
	}

	discardBuf(&text);

	return rc;
      }

      return FALSE;
    }
    default:				/* Number, true, false or PrefixedName */
      if ( is_digit(ts->current_char) )
      { goto case_number;
      } else
      { resource *r;

	if ( (r=read_iri(ts, IRI_BOOL)) )
	{ if ( r == LITERAL_TRUE || r == LITERAL_FALSE )
	  { return got_boolean_triple(ts, r == LITERAL_TRUE);
	  } else
	  { int rc = got_resource_triple(ts, r);
	    free_resource(ts, r);
	    return rc;
	  }
	}

	return FALSE;
      }
  }
}


static int
read_object_list(turtle_state *ts)
{ for(;;)
  { int  rc;

    RECOVER(R_OBJLIST, rc = read_object(ts));

    if ( !skip_ws(ts) )
      return FALSE;

    if ( ts->current_char == ',' )
    { if ( next(ts) )
	continue;
      else
	return FALSE;
    }
    if ( ts->current_char == ';' )
      return TRUE;

    return rc;
  }
}


static int
read_predicate_object_list(turtle_state *ts, const char *end)
{ for(;;)
  { if ( !read_verb(ts)	||
	 !read_object_list(ts) ||
	 !skip_ws(ts) )
      return FALSE;

    if ( ts->current_char == ';' )
    { empty:
      if ( next(ts) && skip_ws(ts) )
      { if ( ts->current_char <= 256 && strchr(end, ts->current_char) )
	  return TRUE;
	if ( ts->current_char == ';' )
	  goto empty;
	continue;
      } else
	return FALSE;
    }

    return TRUE;
  }
}


static int
read_directive_name(turtle_state *ts, string_buffer *b)
{ initBuf(b);

  if ( wcis_pn_chars_base(ts->current_char) )
  { addBuf(b, ts->current_char);
  } else
  { return syntax_error(ts, "Directive name expected");
  }

  for(;;)
  { if ( next(ts) )
    { if ( wcis_pn_chars(ts->current_char) )
      { addBuf(b, ts->current_char);
      } else
      { addBuf(b, EOS);
	return TRUE;
      }
    } else
      return FALSE;
  }
}


static int
read_end_of_clause(turtle_state *ts)
{ if ( skip_ws(ts) &&
       ts->current_char == '.' &&
       next(ts) &&
       ( ts->current_char == -1 ||
	 is_ws(ts->current_char)
       ) )
    return TRUE;

  return syntax_error(ts, "End of statement expected");
}


static int
prefix_directive(turtle_state *ts, int needs_end_of_statement)
{ if ( ts->current_char == ':' )
  { resource *r = NULL;

    if ( next(ts) &&
	 skip_ws(ts) &&
	 (r=read_iri_ref(ts)) &&
	 (!needs_end_of_statement || read_end_of_clause(ts)) )
    { int rc = set_empty_prefix(ts, r);
      free_resource(ts, r);
      return rc;
    }
    if ( r )
      free_resource(ts, r);
  } else
  { string_buffer pn_prefix;

    if ( read_pn_prefix(ts, &pn_prefix) )
    { if ( ts->current_char == ':' )
      { resource *r;

	if ( next(ts) &&
	     skip_ws(ts) &&
	     (r=read_iri_ref(ts)) )
	{ int rc;

	  rc = ( (!needs_end_of_statement || read_end_of_clause(ts)) &&
		 set_prefix(ts, baseBuf(&pn_prefix), r)
	       );
	  free_resource(ts, r);
	  discardBuf(&pn_prefix);

	  return rc;
	}
      } else
	return syntax_error(ts, "Expected \":\"");
    }
  }

  return syntax_error(ts, "Invalid @prefix directive");
}


static int
directive(turtle_state *ts)
{ string_buffer b;

  if ( read_directive_name(ts, &b) )
  { if ( !skip_ws(ts) )
    { discardBuf(&b);
      return FALSE;
    }

    if ( wcscmp(baseBuf(&b), L"base") == 0 )
    { resource *r = NULL;
      int rc;

      discardBuf(&b);

      rc = ( (r=read_iri_ref(ts)) &&
	     read_end_of_clause(ts) &&
	     set_base_uri(ts, r)
	   );
      if ( r )
	free_resource(ts, r);
      if ( rc )
	return TRUE;

      return syntax_error(ts, "Invalid @base directive");
    } else if ( wcscmp(baseBuf(&b), L"prefix") == 0 )
    { discardBuf(&b);
      return prefix_directive(ts, TRUE);
    } else
    { discardBuf(&b);
      return syntax_error(ts, "Unknown directive");
    }
  }

  return syntax_error(ts, "Invalid directive");
}


static int
sparql_base_directive(turtle_state *ts)
{ resource *r;

  if ( skip_ws(ts) &&
       (r=read_iri_ref(ts)) )
  { int rc = set_base_uri(ts, r);
    free_resource(ts, r);

    return rc;
  }

  return FALSE;
}


static int
sparql_prefix_directive(turtle_state *ts)
{ return ( skip_ws(ts) &&
	   prefix_directive(ts, FALSE) );
}


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
final_predicate_object_list() is called from statement() after resolving
the subject.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

static int
final_predicate_object_list(turtle_state *ts)
{ const char *end;

  if ( ts->format == D_TRIG && ts->current_graph )
    end = "}.";
  else
    end = ".";

  if ( !read_predicate_object_list(ts, end) )
    return FALSE;

  if ( ts->current_char == '}' && ts->format == D_TRIG && ts->current_graph )
    return TRUE;

  return read_end_of_clause(ts);
}


static int
starts_graph(turtle_state *ts)
{ if ( ts->current_char == '=' )
  { if ( !next(ts) ||
	 !skip_ws(ts) )
      return -1;			/* input error */
    if ( ts->current_char == '{' )
      return TRUE;
    syntax_error(ts, "TriG: Expected \"{\" after \"=\"");
    return -1;
  }

  if ( ts->current_char == '{' )
    return TRUE;

  return FALSE;
}


static int
graph_or_final_predicate_object_list(turtle_state *ts, resource *r)
{ int rc;

  if ( !skip_ws(ts) )
    return FALSE;

  if ( (rc = starts_graph(ts)) == TRUE )
  { switch ( ts->format )
    { case D_AUTO:
	set_format(ts, D_TRIG);
	/*FALLTHROUGH*/
      case D_TRIG:
	if ( !ts->current_graph )
	{ set_graph(ts, r, NULL);
	  return next(ts) && statement(ts); /* TBD: Return graph(Parser, Graph)? */
	} else
	{ return syntax_error(ts,
			      "TriG: Unexpected \"{\" (nested graphs "
			      "are not allowed)");
	}
      case D_TURTLE:
	syntax_warning(ts, "Unexpected \"<graph> {\" in Turtle format "
		       "(assuming TRiG, ignoring graphs)");
        set_format(ts, D_TRIG_NO_GRAPH);
	/*FALLTHROUGH*/
      case D_TRIG_NO_GRAPH:
	free_resource(ts, r);		/* discard graph name */
	return next(ts) && statement(ts);
      default:
	assert(0);
        return FALSE;
    }
  } else if ( rc == FALSE )
  { set_subject(ts, r, NULL);

    return final_predicate_object_list(ts);
  } else
    return FALSE;			/* error from starts_graph() */
}


static int
statement(turtle_state *ts)
{ retry:

  if ( !skip_ws(ts) )
    return FALSE;

  switch ( ts->current_char )
  { case '@':				/* directive */
    { return next(ts) && directive(ts);
    }
    case '}':
    { if ( ts->format == D_TRIG )
      { set_graph(ts, NULL, NULL);
	return next(ts);			/* return } as empty triple set */
      } else if ( ts->format == D_TRIG_NO_GRAPH )
      { return next(ts);
      } else
	return syntax_error(ts, "Unexpected \"}\" in Turtle format");
    }
    case '{':
    { switch( ts->format )
      { case D_AUTO:
	  set_format(ts, D_TRIG);
	  /*FALLTHROUGH*/
        case D_TRIG:
	  if ( !ts->current_graph )
	  { if ( !next(ts) )
	      return FALSE;
	    set_graph(ts, ts->default_graph, NULL);
	    goto retry;
	  } else
	  { return syntax_error(ts,
				"TriG: Unexpected \"{\" (nested graphs "
				"are not allowed)");
	  }
	case D_TURTLE:
	  syntax_warning(ts, "Unexpected \"{\" in Turtle format "
			 "(assuming TRiG, ignoring graphs)");
	  set_format(ts, D_TRIG_NO_GRAPH);
	  /*FALLTHROUGH*/
        case D_TRIG_NO_GRAPH:
	  if ( !next(ts) )
	    return FALSE;
	  goto retry;
      }
    }
    case '<':
    { resource *r;

      if ( (r=read_iri_ref(ts)) )
	return graph_or_final_predicate_object_list(ts, r);
      return FALSE;
    }
    case '[':
    { resource *r;

      if ( !next(ts) || !skip_ws(ts) )
	return FALSE;
      if ( ts->current_char == ']' )
      { if ( !set_anon_subject(ts, NULL) )
	  return FALSE;
	return next(ts) && final_predicate_object_list(ts);
      } else if ( (r=read_blank_node_property_list(ts)) )
      { if ( !skip_ws(ts) )
	  return FALSE;
	if ( ts->current_char == '.' )
	  return read_end_of_clause(ts);
	set_subject(ts, r, NULL);
	return final_predicate_object_list(ts);
      }
      return syntax_error(ts, "Illegal subject (expected \"]\")");
    }
    case '(':
    { resource *r;

      if ( (r=read_collection(ts)) )
      { set_subject(ts, r, NULL);
	return final_predicate_object_list(ts);
      }
      return FALSE;
    }
    case '_':
    { resource *r;

      if ( (r=read_blank_node_label(ts)) )
      { set_subject(ts, r, NULL);
	return final_predicate_object_list(ts);
      }
      return FALSE;
    }
    case ':':
    { if ( !next(ts) )
	return FALSE;

      if ( pn_local_start(ts->current_char) || starts_plx(ts->current_char) )
      { string_buffer pn_local;

	if ( read_pn_local(ts, &pn_local) )
	{ resource *r;

	  r = resolve_iri(ts, NULL, baseBuf(&pn_local));
	  discardBuf(&pn_local);
	  if ( r )
	    return graph_or_final_predicate_object_list(ts, r);
	}
      } else
      { resource *r;

	if ( (r=resolve_iri(ts, NULL, NULL)) )
	  return graph_or_final_predicate_object_list(ts, r);
      }

      return FALSE;
    }
    case -1:
    { return TRUE;				/* End of file */
    }
    default:
    { string_buffer pn_prefix;

      if ( read_pn_prefix(ts, &pn_prefix) )
      { if ( wcscasecmp(baseBuf(&pn_prefix), L"BASE") == 0 )
	{ discardBuf(&pn_prefix);
	  return sparql_base_directive(ts);
	}
	if ( wcscasecmp(baseBuf(&pn_prefix), L"PREFIX") == 0 )
	{ discardBuf(&pn_prefix);
	  return sparql_prefix_directive(ts);
	}

	if ( ts->current_char == ':' )
	{ if ( !next(ts) )
	  { discardBuf(&pn_prefix);
	    return FALSE;
	  }

	  if ( pn_local_start(ts->current_char) || starts_plx(ts->current_char) )
	  { string_buffer pn_local;

	    if ( read_pn_local(ts, &pn_local) )
	    { resource *r;

	      r = resolve_iri(ts, baseBuf(&pn_prefix), baseBuf(&pn_local));
	      discardBuf(&pn_local);
	      if ( r )
	      { discardBuf(&pn_prefix);
		return graph_or_final_predicate_object_list(ts, r);
	      }
	    }
	  } else
	  { resource *r;

	    r = resolve_iri(ts, baseBuf(&pn_prefix), NULL);
	    discardBuf(&pn_prefix);

	    if ( r )
	      return graph_or_final_predicate_object_list(ts, r);
	  }
	} else
	{ discardBuf(&pn_prefix);
	  return syntax_error(ts, "Expected \":\"");
	}
      }

      return FALSE;
    }
  }
}


		 /*******************************
		 *	  PROLOG BINDING	*
		 *******************************/

#define PARSER_MAGIC 0x536ab5ef

typedef struct parser_symbol
{ int		magic;
  turtle_state *state;
} parser_symbol;


static void
acquire_parser(atom_t symbol)
{
}


static int
release_parser(atom_t symbol)
{ parser_symbol *ps = PL_blob_data(symbol, NULL, NULL);

  free_turtle_parser(ps->state);
  free(ps);

  return TRUE;
}

static int
compare_parsers(atom_t a, atom_t b)
{ parser_symbol *psa = PL_blob_data(a, NULL, NULL);
  parser_symbol *psb = PL_blob_data(b, NULL, NULL);

  return ( psa > psb ?  1 :
	   psb < psa ? -1 : 0
	 );
}

static int
write_parser(IOSTREAM *s, atom_t symbol, int flags)
{ parser_symbol *ps = PL_blob_data(symbol, NULL, NULL);

  Sfprintf(s, "<turtle_parser>(%p)", ps);

  return TRUE;
}


static PL_blob_t turtle_blob =
{ PL_BLOB_MAGIC,
  PL_BLOB_NOCOPY,
  "turtle_parser",
  release_parser,
  compare_parsers,
  write_parser,
  acquire_parser
};


static int
get_turtle_parser(term_t t, turtle_state **ts)
{ PL_blob_t *type;
  void *data;

  if ( PL_get_blob(t, &data, NULL, &type) && type == &turtle_blob)
  { parser_symbol *ps = data;

    assert(ps->magic == PARSER_MAGIC);

    if ( ps->state )
    { *ts = ps->state;

      return TRUE;
    }

    PL_permission_error("access", "destroyed_turtle_parser", t);
    return FALSE;
  }

  return FALSE;
}


static int
unify_turtle_parser(term_t parser, turtle_state *ts)
{ parser_symbol *ps;

  if ( (ps=malloc(sizeof(*ps))) )
  { memset(ps, 0, sizeof(*ps));		/* ensure padding is initialized */
    ps->magic = PARSER_MAGIC;
    ps->state = ts;

    return PL_unify_blob(parser, ps, sizeof(*ps), &turtle_blob);
  }

  return PL_resource_error("memory");
}


static foreign_t
create_turtle_parser(term_t parser, term_t in, term_t options)
{ IOSTREAM *stream;

  if ( PL_get_stream(in, &stream, SIO_INPUT) )
  { turtle_state *ts;

    if ( (ts=new_turtle_parser(stream)) )
    { term_t opt   = PL_new_term_ref();
      term_t arg   = PL_new_term_ref();
      term_t opts  = PL_copy_term_ref(options);

      while(PL_get_list_ex(opts, opt, opts))
      { atom_t name;
	size_t arity;

	if ( PL_get_name_arity(opt, &name, &arity) )
	{ if ( arity == 1 )
	  { _PL_get_arg(1, opt, arg);

	    if ( name == ATOM_base_uri )		/* BASE_URI */
	    { wchar_t *base_uri;
	      resource *r;

	      if ( PL_get_wchars(arg, NULL, &base_uri, CVT_ATOM|CVT_EXCEPTION) &&
		   (r=new_resource(ts, base_uri)) &&
		   set_base_uri(ts, r) )
	      { free_resource(ts, r);
		continue;
	      }

	      return FALSE;
	    }
	    if ( name == ATOM_anon_prefix )		/* ANON_PREFIX */
	    { wchar_t *prefix;

	      if ( PL_is_functor(arg, FUNCTOR_node1) )
	      { if ( ts->bnode.prefix )
		{ free(ts->bnode.prefix);
		  ts->bnode.prefix = NULL;
		}
		continue;
	      } else if ( PL_get_wchars(arg, NULL, &prefix,
					CVT_ATOM|CVT_EXCEPTION) )
	      { if ( ts->bnode.prefix )
		  free(ts->bnode.prefix);
		if ( (ts->bnode.prefix = wcsdup(prefix)) )
		  continue;
		return PL_resource_error("memory");
	      }
	      return FALSE;
	    }
	    if ( name == ATOM_graph )			/* GRAPH */
	    { atom_t g;
	      resource *r;

	      if ( PL_get_atom_ex(arg, &g) &&
		   (r=atom_resource(ts,g)) &&
		   set_graph(ts, r, NULL) )
		continue;
	      return FALSE;
	    }
	    if ( name == ATOM_format )
	    { atom_t d;

	      if ( PL_get_atom_ex(arg, &d) )
	      { if ( d == ATOM_turtle )
		  ts->format = D_TURTLE;
		else if ( d == ATOM_trig )
		  ts->format = D_TRIG;
		else if ( d == ATOM_auto )
		  ts->format = D_AUTO;
		else
		  return PL_domain_error("format_option", arg);

		continue;
	      }
	      return FALSE;
	    }
	    if ( name == ATOM_on_error )
	    { atom_t mode;

	      if ( PL_get_atom_ex(arg, &mode) )
	      { if ( mode == ATOM_error )
		  ts->on_error = E_ERROR;
		else if ( mode == ATOM_warning )
		  ts->on_error = E_WARNING;
		else
		  return PL_domain_error("on_error_option", arg);

		continue;
	      }
	      return FALSE;
	    }
	    continue;			/* ignore unknown option */
	  }
	}
	return PL_type_error("option", opt);
      }
      if ( PL_exception(0) || !PL_get_nil_ex(opts) )
	return FALSE;

      if ( ts->format == D_TRIG &&
	   ts->current_graph )
      { ts->default_graph = ts->current_graph;
	ts->current_graph = NULL;
      }

      if ( unify_turtle_parser(parser, ts) )
	return TRUE;
      free_turtle_parser(ts);
      return FALSE;
    }
  }

  return FALSE;
}


static foreign_t
destroy_turtle_parser(term_t parser)
{ turtle_state *ts;

  if ( get_turtle_parser(parser, &ts) )
  { clear_turtle_parser(ts);

    return TRUE;
  }

  return FALSE;
}


static foreign_t
turtle_parse(term_t parser, term_t triples, term_t options)
{ turtle_state *ts;

  if ( get_turtle_parser(parser, &ts) )
  { term_t tail  = PL_copy_term_ref(triples);
    term_t opt   = PL_new_term_ref();
    term_t arg   = PL_new_term_ref();
    term_t opts  = PL_copy_term_ref(options);
    term_t count = 0;
    int	parse_document = TRUE;

    while(PL_get_list_ex(opts, opt, opts))
    { atom_t name;
      size_t arity;

      if ( PL_get_name_arity(opt, &name, &arity) )
      { if ( arity == 1 )
	{ _PL_get_arg(1, opt, arg);

	  if ( name == ATOM_parse )			/* PARSE */
	  { atom_t what;

	    if ( PL_get_atom_ex(arg, &what) )
	    { if ( what == ATOM_statement )
		parse_document = FALSE;
	      else if ( what == ATOM_document )
		parse_document = TRUE;
	      else
		return PL_domain_error("parse_option", arg);

	      continue;
	    }
	    return FALSE;
	  }
	  if ( name == ATOM_count )			/* COUNT */
	  { count = PL_copy_term_ref(arg);
	    continue;
	  }
	  continue;			/* ignore unknown option */
	}
      }

      return PL_type_error("option", opt);
    }
    if ( PL_exception(0) || !PL_get_nil_ex(opts) )
      return FALSE;

    if ( !count )
    { ts->head = PL_new_term_ref();
      ts->tail = tail;
    }

    if ( parse_document )
    { do
      { statement(ts);
	if ( PL_exception(0) )
	  return FALSE;
      } while( !Sfeof(ts->input) );
    } else
    { statement(ts);
      if ( PL_exception(0) )
	return FALSE;
    }

    ts->head = 0;
    ts->tail = 0;

    if ( count && !PL_unify_int64(count, (int64_t)ts->count) )
      return FALSE;

    return PL_unify_nil(tail);
  }

  return FALSE;
}


/** turtle_graph(+Parser, -Graph) is semidet.
 *
 * True when Graph is the current graph of Parser
*/

static foreign_t
turtle_graph(term_t parser, term_t graph)
{ turtle_state *ts;

  if ( get_turtle_parser(parser, &ts) )
  { if ( ts->current_graph )
    { term_t tmp = PL_new_term_ref();

      if ( put_resource(ts, tmp, ts->current_graph) )
	return PL_unify(graph, tmp);
    }
  }

  return FALSE;
}


/** turtle_set_graph(+Parser, +Graph) is det.
 *
 * Set the notion of the `current graph' for the Turtle parser.
*/

static foreign_t
turtle_set_graph(term_t parser, term_t graph)
{ turtle_state *ts;

  if ( get_turtle_parser(parser, &ts) )
  { atom_t g;

    if ( PL_get_atom_ex(graph, &g) )
    { if ( ts->current_graph )
      { return set_atom_resource(ts->current_graph, g);
      } else
      { if ( (ts->current_graph = atom_resource(ts, g)) )
	  return TRUE;
      }
    }
  }

  return FALSE;
}


static foreign_t
turtle_prefixes(term_t parser, term_t prefixmap)
{ turtle_state *ts;

  if ( get_turtle_parser(parser, &ts) )
  { term_t tail = PL_copy_term_ref(prefixmap);
    term_t head = PL_new_term_ref();
    int e = ts->prefix_map.size;
    int i;

    for(i=0; i<e; i++)
    { hash_cell *c = ts->prefix_map.entries[i];

      for(; c; c=c->next)
      { if ( !PL_unify_list(tail, head, tail) ||
	     !PL_unify_term(head, PL_FUNCTOR, FUNCTOR_pair2,
			      PL_NWCHARS, wcslen(c->name), c->name,
			      PL_NWCHARS, wcslen(c->value.s), c->value.s) )
	  return FALSE;
      }
    }

    return PL_unify_nil(tail);
  }

  return FALSE;
}


static foreign_t
turtle_base(term_t parser, term_t base)
{ turtle_state *ts;

  if ( get_turtle_parser(parser, &ts) )
  { if ( ts->base_uri )
      return PL_unify_wchars(base, PL_ATOM, (size_t)-1, ts->base_uri);
  }

  return FALSE;
}


static foreign_t
turtle_error_count(term_t parser, term_t count)
{ turtle_state *ts;

  if ( get_turtle_parser(parser, &ts) )
  { return PL_unify_int64(count, ts->error_count);
  }

  return FALSE;
}


static foreign_t
turtle_format(term_t parser, term_t format)
{ turtle_state *ts;

  if ( get_turtle_parser(parser, &ts) )
  { atom_t a;

    switch(ts->format)
    { case D_AUTO:          a = ATOM_auto;   break;
      case D_TURTLE:        a = ATOM_turtle; break;
      case D_TRIG:          a = ATOM_trig;   break;
      case D_TRIG_NO_GRAPH: a = ATOM_trig;   break;
      default:
	assert(0);
        return FALSE;
    }

    return PL_unify_atom(format, a);
  }

  return FALSE;
}


		 /*******************************
		 *	    WRITE SUPPORT	*
		 *******************************/

/** turtle_pn_local(+Atom) is semidet.

True if Atom is a valid Turtle local name.
*/

static const char *
skip_plx(const char *s, const char *e)
{ if ( s+3 < e && *s == '%' && hexd(s[1]) >= 0 && hexd(s[2]) >= 0 )
    return s+3;

  return NULL;
}

static int
is_pn_local(const char *s, size_t len)
{ if ( len > 0 )
  { const char *e = &s[len];
    int c = s[0]&0xff;

    if ( pn_local_start(c) || is_local_escape(c) )
      s++;
    else if ( !(s=skip_plx(s, e)) )
      return FALSE;

    while(s<e)
    { int c = s[0]&0xff;

      if ( wcis_pn_chars(c) || c == ':' || is_local_escape(c) )
      { s++;
	continue;
      }
      if ( (s = skip_plx(s,e)) )
	continue;
      if ( c == '.' )
      { if ( s+1 < e )
	{ c = s[1]&0xff;
	  if ( wcis_pn_chars(c) || c == ':' || c == '.' || c == '%' )
	  { s++;
	    continue;
	  }
	}
      }
      return FALSE;
    }
  }
  return TRUE;
}


static const pl_wchar_t *
wskip_plx(const pl_wchar_t *s, const pl_wchar_t *e)
{ if ( s+3 < e && *s == '%' && hexd(s[1]) >= 0 && hexd(s[2]) >= 0 )
    return s+3;

  return NULL;
}


static int
wis_pn_local(const pl_wchar_t *s, size_t len)
{ if ( len > 0 )
  { const pl_wchar_t *e = &s[len];
    int c = s[0];

    if ( pn_local_start(c) || is_local_escape(c) )
      s++;
    else if ( !(s=wskip_plx(s, e)) )
      return FALSE;

    while(s<e)
    { int c = s[0];

      if ( wcis_pn_chars(c) || c == ':' || is_local_escape(c) )
      { s++;
	continue;
      }
      if ( (s = wskip_plx(s,e)) )
	continue;
      if ( c == '.' )
      { if ( s+1 < e )
	{ c = s[1];
	  if ( wcis_pn_chars(c) || c == ':' || c == '.' || c == '%' )
	  { s++;
	    continue;
	  }
	}
      }
      return FALSE;
    }
  }
  return TRUE;
}




static foreign_t
turtle_pn_local(term_t name)
{ char *s;
  pl_wchar_t *w;
  size_t len;

  if ( PL_get_nchars(name, &len, &s, CVT_ATOM) )
    return is_pn_local(s, len);
  else if ( PL_get_wchars(name, &len, &w, CVT_ATOM|CVT_EXCEPTION) )
    return wis_pn_local(w, len);
  else
    return FALSE;
}


static foreign_t
iri_turtle_prefix(term_t iri, term_t prefix)
{ char *s;
  pl_wchar_t *w;
  size_t len;

  if ( PL_get_nchars(iri, &len, &s, CVT_ATOM) )
  { const char *e = &s[len]-1;

    while(e>s && e[0] != '/' && e[0] != '#')
      e--;
    if ( e < &s[len] && (e[0] == '/' || e[0] == '#') )
      e++;
    if ( is_pn_local(e, &s[len]-e) )
      return PL_unify_atom_nchars(prefix, e-s, s);
  } else if ( PL_get_wchars(iri, &len, &w, CVT_ATOM|CVT_EXCEPTION) )
  { const pl_wchar_t *e = &w[len]-1;

    while(e>w && e[0] != '/' && e[0] != '#')
      e--;
    if ( e < &w[len] && (e[0] == '/' || e[0] == '#') )
      e++;
    if ( wis_pn_local(e, &w[len]-e) )
      return PL_unify_wchars(prefix, PL_ATOM, e-w, w);
  }

  return FALSE;
}



		 /*******************************
		 *	     WRITING		*
		 *******************************/

static int
ttl_put_uesc(IOSTREAM *s, int c)
{ if ( c <= 0xffff )
    return Sfprintf(s, "\\u%04x", (unsigned)c);
  else
    return Sfprintf(s, "\\U%08x", (unsigned)c);
}


static int
ttl_put_character(IOSTREAM *s, int c)
{ if ( c >= 32 && c <= 126 )
    return Sputcode(c, s);
  if ( c <= 31 )
    return ttl_put_uesc(s, c);
  if ( c >= 127 && c < 0x10ffff )
  { if ( s->encoding == ENC_ASCII )
      return ttl_put_uesc(s, c);
    if ( s->encoding == ENC_ISO_LATIN_1 && c > 255 )
      return ttl_put_uesc(s, c);
    return Sputcode(c, s);
  }

  PL_representation_error("turtle_character");
  return -1;
}


static int
ttl_put_echaracter(IOSTREAM *s, int c)
{ int c2;
  int rc;

  switch(c)
  { case '\t': c2 = 't'; break;
    case '\n': c2 = 'n'; break;
    case '\r': c2 = 'r'; break;
    default:
      return ttl_put_character(s, c);
  }

  if ( (rc=Sputcode('\\', s)) < 0 )
    return rc;

  return Sputcode(c2, s);
}


static int
ttl_put_scharacter(IOSTREAM *s, int c)
{ int rc;

  switch(c)
  { case '"':
      if ( (rc=Sputcode('\\', s)) < 0 )
	return rc;
      return Sputcode('"', s);
    case '\\':
      if ( (rc=Sputcode('\\', s)) < 0 )
	return rc;
      return Sputcode('\\', s);
    default:
      return ttl_put_echaracter(s, c);
  }
}


static void
write_long_q(IOSTREAM *out)
{ Sputcode('"', out);
  Sputcode('"', out);
  Sputcode('"', out);
}

#define StryPutcode(c, s) \
	do { if ( Sputcode(c,s) < 0 ) goto error; } while(0)

static foreign_t
turtle_write_quoted_string(term_t Stream, term_t Value, term_t wlong)
{ size_t len;
  char *textA;
  pl_wchar_t *textW;
  IOSTREAM *out;
  int vwlong;
  int write_long = -1;

  if ( !(vwlong=PL_is_variable(wlong)) && !PL_get_bool_ex(wlong, &write_long) )
    return FALSE;
  if ( !PL_get_stream_handle(Stream, &out) )
    return FALSE;

  if ( PL_get_nchars(Value, &len, &textA, CVT_ATOM|CVT_STRING) )
  { const char *e = &textA[len];
    const char *s;

    if ( write_long == -1 )
    { for(s=textA; s<e; s++)
      { if ( *s == '\r' || *s == '\n' )
	{ write_long = TRUE;
	  break;
	}
      }
      if ( write_long == -1 )
	write_long = FALSE;
    }

    if ( vwlong && !PL_unify_bool(wlong, write_long) )
    { PL_release_stream(out);
      return FALSE;
    }

    if ( write_long )
    { write_long_q(out);
      for(s=textA; s<e; s++)
      { if ( *s == '"' && ((s+1<e && s[1] != '"') || (s+2<e && s[2] != '"')) )
	  StryPutcode('"', out);
	else if ( *s == '\n' || *s == '\r' )
	  StryPutcode(*s, out);
	else if ( ttl_put_scharacter(out, s[0]&0xff) < 0 )
	  goto error;
      }
      write_long_q(out);
    } else
    { StryPutcode('"', out);
      for(s=textA; s<e; s++)
      { if ( ttl_put_scharacter(out, s[0]&0xff) < 0 )
	  goto error;
      }
      StryPutcode('"', out);
    }

    return PL_release_stream(out);
  } else if ( PL_get_wchars(Value, &len, &textW,
			    CVT_ATOM|CVT_STRING|CVT_EXCEPTION) )
  { const pl_wchar_t *e = &textW[len];
    const pl_wchar_t *w;

    if ( write_long == -1 )
    { for(w=textW; w<e; w++)
      { if ( *w == '\r' || *w == '\n' )
	{ write_long = TRUE;
	  break;
	}
      }
      if ( write_long == -1 )
	write_long = FALSE;
    }

    if ( vwlong && !PL_unify_bool(wlong, write_long) )
    { PL_release_stream(out);
      return FALSE;
    }

    if ( write_long )
    { write_long_q(out);
      for(w=textW; w<e; w++)
      { if ( *w == '"' && ((w+1<e && w[1] != '"') || (w+2<e && w[2] != '"')) )
	  StryPutcode('"', out);
	else if ( *w == '\n' || *w == '\r' )
	  StryPutcode(*w, out);
	else if ( ttl_put_scharacter(out, w[0]) < 0 )
	  goto error;
      }
      write_long_q(out);
    } else
    { StryPutcode('"', out);
      for(w=textW; w<e; w++)
      { if ( ttl_put_scharacter(out, w[0]) < 0 )
	  goto error;
      }
      StryPutcode('"', out);
    }

    return PL_release_stream(out);
  } else
  { error:
    PL_release_stream(out);
    return FALSE;
  }
}


static int
ttl_put_ucharacter(IOSTREAM *s, int c)
{ int rc;

  switch(c)
  { case '>':				/* not allowed in rfc3987.txt */
    case '<':
    case '\\':
    case '^':
    case '|':
    case '{':
    case '}':
    case '"':
    case '`':
    case ' ':
      if ( (rc=Sfprintf(s, "%%%02x", c)) < 0 )
	return rc;
      return 0;
    default:
      return ttl_put_character(s, c);
  }
}


/** turtle_write_uri(+Stream, +URI) is det.
*/

static foreign_t
turtle_write_uri(term_t Stream, term_t Value)
{ size_t len;
  char *s;
  pl_wchar_t *w;
  IOSTREAM *out;

  if ( !PL_get_stream_handle(Stream, &out) )
    return FALSE;

  if ( PL_get_nchars(Value, &len, &s, CVT_ATOM|CVT_STRING) )
  { const char *e = &s[len];

    StryPutcode('<', out);
    for(; s<e; s++)
    { if ( ttl_put_ucharacter(out, s[0]&0xff) < 0 )
	goto error;
    }
    StryPutcode('>', out);
    return PL_release_stream(out);
  } else if ( PL_get_wchars(Value, &len, &w, CVT_ATOM|CVT_EXCEPTION) )
  { const pl_wchar_t *e = &w[len];

    StryPutcode('<', out);
    for(; w<e; w++)
    { if ( ttl_put_ucharacter(out, w[0]) < 0 )
	goto error;
    }
    StryPutcode('>', out);
    return PL_release_stream(out);
  } else
  { error:
    PL_release_stream(out);
    return FALSE;
  }
}


/* Local escape characters are: ~.-!$&'()*+,;=/?#@%_
*/

static inline int
wr_is_local_escape(int c)
{ return is_local_escape(c) && !strchr("_-%", c);
}

static foreign_t
turtle_write_pn_local(term_t Stream, term_t Value)
{ size_t len;
  char *s;
  pl_wchar_t *w;
  IOSTREAM *out;

  if ( !PL_get_stream_handle(Stream, &out) )
    return FALSE;

  if ( PL_get_nchars(Value, &len, &s, CVT_ATOM|CVT_STRING) )
  { if ( len > 0 )
    { const char *e = &s[len];
      int c = s[0]&0xff;

      if ( !pn_local_start(c) )
	StryPutcode('\\', out);
      StryPutcode(c, out);

      for(s++; s<e; s++)
      { c = s[0]&0xff;

	if ( c == '.' && s+1 < e && !strchr(":.%", s[1]) )
	{ StryPutcode(c, out);
	} else
	{ if ( wr_is_local_escape(c) )
	    StryPutcode('\\', out);
	  StryPutcode(c, out);
	}
      }
    }
    return PL_release_stream(out);
  } else if ( PL_get_wchars(Value, &len, &w, CVT_ATOM|CVT_EXCEPTION) )
  { if ( len > 0 )
    { const pl_wchar_t *e = &w[len];
      int c = w[0];

      if ( !pn_local_start(c) )
	StryPutcode('\\', out);
      StryPutcode(c, out);

      for(w++; w<e; w++)
      { c = w[0];

	if ( c == '.' && w+1 < e && !strchr(":.%", w[1]) )
	{ StryPutcode(c, out);
	} else
	{ if ( wr_is_local_escape(c) )
	    StryPutcode('\\', out);
	  StryPutcode(c, out);
	}
      }
    }
    return PL_release_stream(out);
  } else
  { error:
    PL_release_stream(out);
    return FALSE;
  }
}


		 /*******************************
		 *	      REGISTER		*
		 *******************************/

#define MKATOM(n) \
	ATOM_ ## n = PL_new_atom(#n)
#define MKFUNCTOR(n, a) \
	FUNCTOR_ ## n ## a = PL_new_functor(PL_new_atom(#n), a)

install_t
install_turtle(void)
{ FUNCTOR_pair2  = PL_new_functor(PL_new_atom("-"), 2);
  FUNCTOR_colon2 = PL_new_functor(PL_new_atom(":"), 2);

  MKFUNCTOR(error,	     2);
  MKFUNCTOR(syntax_error,    1);
  MKFUNCTOR(existence_error, 2);
  MKFUNCTOR(stream,	     4);
  MKFUNCTOR(rdf,	     3);
  MKFUNCTOR(rdf,	     4);
  MKFUNCTOR(literal,	     1);
  MKFUNCTOR(lang,	     2);
  MKFUNCTOR(type,	     2);
  MKFUNCTOR(node,	     1);

  MKATOM(parse);
  MKATOM(statement);
  MKATOM(document);
  MKATOM(count);
  MKATOM(error_count);
  MKATOM(anon_prefix);
  MKATOM(base_uri);
  MKATOM(on_error);
  MKATOM(error);
  MKATOM(warning);
  MKATOM(format);
  MKATOM(turtle);
  MKATOM(trig);
  MKATOM(graph);
  MKATOM(auto);

  PL_register_foreign("create_turtle_parser",  3, create_turtle_parser,  0);
  PL_register_foreign("destroy_turtle_parser", 1, destroy_turtle_parser, 0);
  PL_register_foreign("turtle_parse",          3, turtle_parse,          0);
  PL_register_foreign("turtle_prefixes",       2, turtle_prefixes,       0);
  PL_register_foreign("turtle_base",           2, turtle_base,           0);
  PL_register_foreign("turtle_error_count",    2, turtle_error_count,    0);

  PL_register_foreign("turtle_set_graph",      2, turtle_set_graph,      0);
  PL_register_foreign("turtle_graph",	       2, turtle_graph,          0);
  PL_register_foreign("turtle_format",	       2, turtle_format,         0);

  PL_register_foreign("turtle_pn_local",       1, turtle_pn_local,       0);
  PL_register_foreign("iri_turtle_prefix",     2, iri_turtle_prefix,     0);
  PL_register_foreign("turtle_write_uri",      2, turtle_write_uri,      0);
  PL_register_foreign("turtle_write_pn_local", 2, turtle_write_pn_local, 0);
  PL_register_foreign("turtle_write_quoted_string",
					    3, turtle_write_quoted_string, 0);
}
