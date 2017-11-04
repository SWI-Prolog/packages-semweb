/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2013-2015, VU University Amsterdam
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
#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <string.h>
#include <assert.h>
#include "turtle_chars.c"

static atom_t ATOM_end_of_file;

static functor_t FUNCTOR_node1;
static functor_t FUNCTOR_literal1;
static functor_t FUNCTOR_type2;
static functor_t FUNCTOR_lang2;
static functor_t FUNCTOR_triple3;
static functor_t FUNCTOR_quad4;
static functor_t FUNCTOR_error2;
static functor_t FUNCTOR_syntax_error1;
static functor_t FUNCTOR_stream4;


		 /*******************************
		 *	CHARACTER CLASSES	*
		 *******************************/

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
is_ws(int c)				/* Turtle: (WS|EL) */
{ return (c < 128 ? (char_type[c] & (WS)) != 0 : FALSE);
}

static inline int
is_eol(int c)
{ return (c < 128 ? (char_type[c] & EL) != 0 : FALSE);
}

static inline int
is_lang_char1(int c)
{ return (c < 128 ? (char_type[c] & (LC|UC)) != 0 : FALSE);
}

static inline int
is_lang_char(int c)
{ return (c < 128 ? (char_type[c] & (LC|UC|DI)) != 0 : FALSE) || c == '-';
}

static const signed char hexval0[] =
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

static const signed char* hexval = &hexval0[1];

static inline int
hexd(int c)
{ return (c <= 'f' ? hexval[c] : -1);
}


static inline int
wcis_pn_chars_u(int c)			/* 158s */
{ return ( wcis_pn_chars_base(c) ||
	   c == '_' || c == ':'
	 );
}


static inline int			/* 141s RDF Blank Nodes */
wcis_pn_chars_du(int c)
{ return ( wcis_pn_chars_u(c) ||
	   (c >= '0' && c <= '9')
	 );
}

static inline int
wcis_pn_chars(int c)			/* 160s */
{ return ( wcis_pn_chars_u(c) ||
	   c == '-' ||
	   (c >= '0' && c <= '9') ||
	   wcis_pn_chars_extra(c)
	 );
}

		 /*******************************
		 *	      ERROR		*
		 *******************************/

static int
syntax_error(IOSTREAM *in, const char *msg)
{ term_t ex = PL_new_term_refs(2);
  IOPOS *pos;

  if ( !PL_unify_term(ex+0, PL_FUNCTOR, FUNCTOR_syntax_error1,
		              PL_CHARS, msg) )
    return FALSE;

  if ( (pos=in->position) )
  { term_t stream;

    if ( !(stream = PL_new_term_ref()) ||
	 !PL_unify_stream(stream, in) ||
	 !PL_unify_term(ex+1,
			PL_FUNCTOR, FUNCTOR_stream4,
			  PL_TERM, stream,
			  PL_INT, (int)pos->lineno,
			  PL_INT, (int)(pos->linepos-1), /* one too late */
			  PL_INT64, (int64_t)(pos->charno-1)) )
      return FALSE;
  }

  if ( PL_cons_functor_v(ex, FUNCTOR_error2, ex) )
  { int c;

    do
    { c = Sgetcode(in);
    } while(c != '\n' && c != -1);

    return PL_raise_exception(ex);
  }

  return FALSE;
}


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

  PL_resource_error("memory");
  return FALSE;
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
		 *	     SKIPPING		*
		 *******************************/

static int
skip_ws(IOSTREAM *in, int *cp)
{ int c = *cp;

  while(is_ws(c))
    c = Sgetcode(in);

  *cp = c;

  return !Sferror(in);
}


static int
skip_comment_line(IOSTREAM *in, int *cp)
{ int c;

  do
  { c = Sgetcode(in);
  } while ( c != -1 && !is_eol(c) );

  while(is_eol(c))
    c = Sgetcode(in);

  *cp = c;

  return !Sferror(in);
}


static int
skip_eol(IOSTREAM *in, int *cp)
{ if ( skip_ws(in, cp) )
  { int c = *cp;

    if ( c == '\n' )
      return TRUE;
    if ( c == '\r' )
    { if ( Speekcode(in) == '\n' )
	(void)Sgetcode(in);
      return TRUE;
    }
    if ( c == EOF )
      return TRUE;
    if ( c == '#' )
      return skip_comment_line(in, cp);

    return syntax_error(in, "end-of-line expected");
  } else
  { return FALSE;
  }
}


		 /*******************************
		 *	      READING		*
		 *******************************/

#define ESCAPED_CODE (-1)

static int
read_hex(IOSTREAM *in, int *cp, int len)
{ int c = 0;

  while(len-- > 0)
  { int v0;
    int c2 = Sgetcode(in);

    if ( (v0 = hexd(c2)) >= 0 )
    { c <<= 4;
      c += v0;
    } else
    { return syntax_error(in, "illegal unicode escape");
    }
  }

  *cp = c;
  return ESCAPED_CODE;
}


static int
get_iri_code(IOSTREAM *in, int *cp)
{ int c = Sgetcode(in);

  switch(c)
  { case '\r':
    case '\n':
      return syntax_error(in, "newline in uriref");
    case EOF:
      return syntax_error(in, "EOF in uriref");
    case '<':
    case '"':
    case '{':
    case '}':
    case '|':
    case '^':
    case '`':
      return syntax_error(in, "Illegal character in uriref");
    case '\\':
    { int c2 = Sgetcode(in);

      switch(c2)
      { case 'u':	return read_hex(in, cp, 4);
	case 'U':	return read_hex(in, cp, 8);
	default:	return syntax_error(in, "illegal escape");
      }
    }
    default:
    { if ( c > ' ' )
      { *cp = c;
	return TRUE;
      }
      return syntax_error(in, "Illegal control character in uriref");
    }
  }
}

static int
read_uniref(IOSTREAM *in, term_t subject, int *cp)
{ int c;
  string_buffer buf;

  initBuf(&buf);
  for(;;)
  { int rc;

    if ( (rc=get_iri_code(in, &c)) == TRUE )
    { switch(c)
      { case '>':
	{ int rc = PL_unify_wchars(subject, PL_ATOM,
				   bufSize(&buf), baseBuf(&buf));
	  discardBuf(&buf);
	  *cp = Sgetcode(in);
	  return rc;
	}
	default:
	  if ( !addBuf(&buf, c) )
	  { discardBuf(&buf);
	    return FALSE;
	  }
      }
    } else if ( rc == ESCAPED_CODE )
    { if ( !addBuf(&buf, c) )
      { discardBuf(&buf);
	return FALSE;
      }
    } else
    { discardBuf(&buf);
      return FALSE;
    }
  }
}


static int
read_node_id(IOSTREAM *in, term_t subject, int *cp)
{ int c;

  c = Sgetcode(in);
  if ( c != ':' )
    return syntax_error(in, "invalid nodeID");

  c = Sgetcode(in);
  if ( wcis_pn_chars_du(c) )
  { string_buffer buf;

    initBuf(&buf);
    addBuf(&buf, c);
    for(;;)
    { int c2;

      c = Sgetcode(in);

      if ( wcis_pn_chars(c) )
      { addBuf(&buf, c);
      } else if ( c == '.' &&
		  (wcis_pn_chars((c2=Speekcode(in))) || c2 == '.') )
      { addBuf(&buf, c);
      } else
      { term_t av = PL_new_term_refs(1);
	int rc;

	rc = ( PL_unify_wchars(av+0, PL_ATOM, bufSize(&buf), baseBuf(&buf)) &&
	       PL_cons_functor_v(subject, FUNCTOR_node1, av)
	     );
	discardBuf(&buf);
	*cp = c;

	return rc;
      }
    }
  } else
    return syntax_error(in, "invalid nodeID");
}


static int
read_lan(IOSTREAM *in, term_t lan, int *cp)
{ int c;
  string_buffer buf;
  int rc;

  c = Sgetcode(in);
  if ( !skip_ws(in, &c) )
    return FALSE;
  if ( !is_lang_char1(c) )
    return syntax_error(in, "language tag must start with a-zA-Z");

  initBuf(&buf);
  addBuf(&buf, c);
  for(;;)
  { c = Sgetcode(in);
    if ( is_lang_char(c) )
    { addBuf(&buf, c);
    } else
    { break;
    }
  }
  while(c=='-')
  { addBuf(&buf, c);
    c = Sgetcode(in);
    if ( !is_lang_char(c) )
    { discardBuf(&buf);
      return syntax_error(in, "Illegal language tag");
    }
    addBuf(&buf, c);
    for(;;)
    { c = Sgetcode(in);
      if ( is_lang_char(c) )
      { addBuf(&buf, c);
      } else
      { break;
      }
    }
  }

  *cp = c;
  rc = PL_unify_wchars(lan, PL_ATOM, bufSize(&buf), baseBuf(&buf));
  discardBuf(&buf);

  return rc;
}


static int
read_subject(IOSTREAM *in, term_t subject, int *cp)
{ int c = *cp;
  int rc;

  switch ( c )
  { case '<':
      rc = read_uniref(in, subject, cp);
      break;
    case '_':
      rc = read_node_id(in, subject, cp);
      break;
    default:
      return syntax_error(in, "subject expected");
  }

  if ( rc && !is_ws(*cp) )
    return syntax_error(in, "subject not followed by whitespace");

  return rc;
}


static int
read_predicate(IOSTREAM *in, term_t predicate, int *cp)
{ int c = *cp;
  int rc;

  switch ( c )
  { case '<':
      rc = read_uniref(in, predicate, cp);
      break;
    default:
      return syntax_error(in, "predicate expected");
  }

  if ( rc && !is_ws(*cp) )
    return syntax_error(in, "predicate not followed by whitespace");

  return rc;
}


static int
read_graph(IOSTREAM *in, term_t graph, int *cp)
{ int c = *cp;
  int rc;

  switch ( c )
  { case '<':
      rc = read_uniref(in, graph, cp);
      break;
    default:
      return syntax_error(in, "graph expected");
  }

  return rc;
}


static int
wrap_literal(term_t lit)
{ return PL_cons_functor_v(lit, FUNCTOR_literal1, lit);
}


static int
get_string_code(IOSTREAM *in, int *cp)
{ int c = Sgetcode(in);

  switch(c)
  { case '\r':
    case '\n':
      return syntax_error(in, "newline in string");
    case '\\':
    { int c2 = Sgetcode(in);

      switch(c2)
      { case 'b':	*cp = '\b'; return ESCAPED_CODE;
	case 't':	*cp = '\t'; return ESCAPED_CODE;
	case 'f':	*cp = '\f'; return ESCAPED_CODE;
	case 'n':	*cp = '\n'; return ESCAPED_CODE;
	case 'r':	*cp = '\r'; return ESCAPED_CODE;
	case '"':	*cp =  '"'; return ESCAPED_CODE;
	case '\\':	*cp = '\\'; return ESCAPED_CODE;
	case 'u':	return read_hex(in, cp, 4);
	case 'U':	return read_hex(in, cp, 8);
	default:	return syntax_error(in, "illegal escape");
      }
    }
    default:
      *cp = c;
      return TRUE;
  }
}


static int
read_literal(IOSTREAM *in, term_t literal, int *cp)
{ int c;
  string_buffer buf;

  initBuf(&buf);
  for(;;)
  { int rc;

    if ( (rc=get_string_code(in, &c)) == TRUE )
    { switch(c)
      { case '"':
	{ c = Sgetcode(in);

	  if ( !skip_ws(in, &c) )
	  { discardBuf(&buf);
	    return FALSE;
	  }

	  switch(c)
	  { case '@':
	    { term_t av = PL_new_term_refs(2);

	      if ( read_lan(in, av+0, cp) )
	      { int rc = ( PL_unify_wchars(av+1, PL_ATOM,
					   bufSize(&buf), baseBuf(&buf)) &&
			   PL_cons_functor_v(literal, FUNCTOR_lang2, av) &&
			   wrap_literal(literal)
			 );
		discardBuf(&buf);
		return rc;
	      } else
	      { discardBuf(&buf);
		return FALSE;
	      }
	    }
	    case '^':
	    { c = Sgetcode(in);

	      if ( c == '^' )
	      { term_t av = PL_new_term_refs(2);

		c = Sgetcode(in);
		if ( !skip_ws(in, &c) )
		{ discardBuf(&buf);
		  return FALSE;
		}
		if ( c == '<' )
		{ if ( read_uniref(in, av+0, cp) )
		  { int rc = ( PL_unify_wchars(av+1, PL_ATOM,
					       bufSize(&buf), baseBuf(&buf)) &&
			       PL_cons_functor_v(literal, FUNCTOR_type2, av) &&
			       wrap_literal(literal)
			     );
		    discardBuf(&buf);
		    return rc;
		  } else
		  { discardBuf(&buf);
		    return FALSE;
		  }
		} else
		{ discardBuf(&buf);
		  return syntax_error(in, "datatype uriref expected");
		}
	      } else
	      { discardBuf(&buf);
		return syntax_error(in, "^ expected");
	      }
	    }
	    default:
	    { int rc;

	      *cp = c;
	      rc = ( PL_unify_wchars(literal, PL_ATOM,
				     bufSize(&buf), baseBuf(&buf)) &&
		     wrap_literal(literal)
		   );
	      discardBuf(&buf);
	      return rc;
	    }
	  }
	}
	case EOF:
	  discardBuf(&buf);
	  return syntax_error(in, "EOF in string");
	case '\n':
	case '\r':
	  discardBuf(&buf);
	  return syntax_error(in, "newline in string");
	default:
	  if ( !addBuf(&buf, c) )
	  { discardBuf(&buf);
	    return FALSE;
	  }
      }
    } else if ( rc == ESCAPED_CODE )
    { if ( !addBuf(&buf, c) )
      { discardBuf(&buf);
	return FALSE;
      }
    } else
    { discardBuf(&buf);
      return FALSE;
    }
  }
}


static int
read_object(IOSTREAM *in, term_t object, int *cp)
{ int c = *cp;

  switch ( c )
  { case '<':
      return read_uniref(in, object, cp);
    case '_':
      return read_node_id(in, object, cp);
    case '"':
      return read_literal(in, object, cp);
    default:
      return syntax_error(in, "object expected");
  }
}


static int
check_full_stop(IOSTREAM *in, int *cp)
{ int c = *cp;

  if ( c == '.' )
  { *cp = Sgetcode(in);

    return TRUE;
  }

  return syntax_error(in, "fullstop (.) expected");
}


static int
read_ntuple(term_t from, term_t triple, int arity)
{ IOSTREAM *in;
  int rc;
  int c;

  if ( !PL_get_stream(from, &in, SIO_INPUT) )
    return FALSE;

  c=Sgetcode(in);
next:
  rc = skip_ws(in, &c);

  if ( rc )
  { if ( c == '#' )
    { if ( skip_comment_line(in, &c) )
	goto next;
    } else if ( c == EOF )
    { rc = PL_unify_atom(triple, ATOM_end_of_file);
    } else if ( c < 128 && (char_type[c]&EL) )
    { if ( skip_eol(in, &c) )
      { c=Sgetcode(in);
	goto next;
      } else
	return FALSE;
    } else
    { term_t av = PL_new_term_refs(5);	/* room for quad */

      rc = (  read_subject(in, av+1, &c) &&
	      skip_ws(in, &c) &&
	      read_predicate(in, av+2, &c) &&
	      skip_ws(in, &c) &&
	      read_object(in, av+3, &c) &&
	      skip_ws(in, &c)
	   );

      if ( rc )
      {
      again:
	if ( arity == 3 )
	{ rc = ( check_full_stop(in, &c) &&
		 skip_eol(in, &c)
	       );
	} else if ( arity == 4 )
	{ rc = ( read_graph(in, av+4, &c) &&
		 skip_ws(in, &c) &&
		 check_full_stop(in, &c) &&
		 skip_eol(in, &c)
	       );
	} else
	{ arity = (c == '<' ? 4 : 3);
	  goto again;
	}
      }

      if ( rc )
      { functor_t f = arity == 3 ? FUNCTOR_triple3 : FUNCTOR_quad4;

	rc = ( PL_cons_functor_v(av+0, f, av+1) &&
	       PL_unify(triple, av+0)
	     );
      }
    }
  }

  return (PL_release_stream(in) && rc);
}


static foreign_t
read_ntriple(term_t from, term_t triple)
{ return read_ntuple(from, triple, 3);
}

static foreign_t
read_nquad(term_t from, term_t quad)
{ return read_ntuple(from, quad, 4);
}

static foreign_t
read_ntuple2(term_t from, term_t quad)
{ return read_ntuple(from, quad, 0);
}


		 /*******************************
		 *	       INSTALL		*
		 *******************************/

#define MKFUNCTOR(n, a) \
	FUNCTOR_ ## n ## a = PL_new_functor(PL_new_atom(#n), a)

install_t
install_ntriples(void)
{ ATOM_end_of_file = PL_new_atom("end_of_file");

  MKFUNCTOR(node,         1);
  MKFUNCTOR(literal,      1);
  MKFUNCTOR(type,         2);
  MKFUNCTOR(lang,         2);
  MKFUNCTOR(triple,       3);
  MKFUNCTOR(quad,         4);
  MKFUNCTOR(error,        2);
  MKFUNCTOR(syntax_error, 1);
  MKFUNCTOR(stream,       4);

  PL_register_foreign("read_ntriple", 2, read_ntriple, 0);
  PL_register_foreign("read_nquad",   2, read_nquad,   0);
  PL_register_foreign("read_ntuple",  2, read_ntuple2, 0);
}
