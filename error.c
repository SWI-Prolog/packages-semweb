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

#include "rdf_db.h"


		 /*******************************
		 *	       ERRORS		*
		 *******************************/

static functor_t FUNCTOR_error2;
static functor_t FUNCTOR_literal1;
static functor_t FUNCTOR_colon2;

int
permission_error(const char *op, const char *type, const char *obj,
		 const char *msg)
{ term_t ex, ctx;

  if ( !(ex = PL_new_term_ref()) ||
       !(ctx = PL_new_term_ref()) )
    return FALSE;

  if ( msg )
  { if ( !PL_unify_term(ctx, PL_FUNCTOR_CHARS, "context", 2,
			       PL_VARIABLE,
			       PL_CHARS, msg) )
      return FALSE;
  }

  if ( !PL_unify_term(ex, PL_FUNCTOR_CHARS, "error", 2,
		      PL_FUNCTOR_CHARS, "permission_error", 3,
		        PL_CHARS, op,
		        PL_CHARS, type,
		        PL_CHARS, obj,
		      PL_TERM, ctx) )
    return FALSE;

  return PL_raise_exception(ex);
}


int
is_literal(term_t t)
{ return PL_is_functor(t, FUNCTOR_literal1);
}


typedef struct prefix_cache
{ atom_t local;
  atom_t alias;
  atom_t uri;
  int    generation;
  int	 locked;
} prefix_cache;

#define PREFIX_EXPAND_ENTRIES 4

static prefix_cache cache[PREFIX_EXPAND_ENTRIES] = {{0}};
static int cache_ptr = 0;

static atom_t
cached_expansion(atom_t alias, atom_t local)
{ prefix_cache *c;
  int i;

  for(i=0, c=cache; i<PREFIX_EXPAND_ENTRIES; i++, c++)
  { int gen0 = c->generation;
    atom_t uri = c->uri;

    if ( c->local == local && c->alias == alias &&
	 gen0 == c->generation )
      return uri;
  }

  return (atom_t)0;
}

static void
cache_expansion(atom_t alias, atom_t local, atom_t uri)
{ int i;

  for(i=(++cache_ptr%PREFIX_EXPAND_ENTRIES); ; i = (i+1)%PREFIX_EXPAND_ENTRIES)
  { prefix_cache *c = &cache[i];

    if ( __sync_bool_compare_and_swap(&c->locked, 0, 1) )
    { atom_t olocal = c->local;
      atom_t ouri   = c->uri;

      c->local = 0;
      c->alias = 0;
      c->uri   = 0;
      c->generation++;
      c->uri   = uri;
      c->alias = alias;
      c->local = local;

      PL_register_atom(local);
      PL_register_atom(uri);
      if ( olocal) PL_unregister_atom(olocal);
      if ( ouri)   PL_unregister_atom(ouri);

      c->locked = 0;

      return;
    }
  }
}


void
flush_prefix_cache(void)
{ int i;

  for( i=0; i<PREFIX_EXPAND_ENTRIES; i++)
  { prefix_cache *c = &cache[i];

    while( !__sync_bool_compare_and_swap(&c->locked, 0, 1) )
      ;

    { atom_t olocal = c->local;
      atom_t ouri   = c->uri;

      c->local = 0;
      c->alias = 0;
      c->uri   = 0;
      c->generation++;

      if ( olocal) PL_unregister_atom(olocal);
      if ( ouri)   PL_unregister_atom(ouri);

      c->locked = 0;
    }
  }
}


int
get_prefixed_iri(rdf_db *db, term_t t, atom_t *ap)
{ if ( PL_is_functor(t, FUNCTOR_colon2) )
  { term_t a = PL_new_term_ref();
    atom_t alias, local, uri;

    _PL_get_arg(1, t, a);
    if ( !PL_get_atom(a, &alias) )
      return FALSE;
    _PL_get_arg(2, t, a);
    if ( !PL_get_atom(a, &local) )
      return FALSE;

    if ( (uri = cached_expansion(alias, local)) )
    { *ap = uri;
      return TRUE;
    }

    if ( (uri = expand_prefix(db, alias, local)) )
    { cache_expansion(alias, local, uri);

      *ap = uri;
      return TRUE;
    }
  }

  return FALSE;
}


#define MKFUNCTOR(n, a) \
	FUNCTOR_ ## n ## a = PL_new_functor(PL_new_atom(#n), a)

int
init_errors(void)
{ MKFUNCTOR(error, 2);
  MKFUNCTOR(literal, 1);
  FUNCTOR_colon2 = PL_new_functor(PL_new_atom(":"), 2);

  return TRUE;
}
