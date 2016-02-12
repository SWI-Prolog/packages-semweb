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

#ifndef ERROR_H_INCLUDED
#define ERROR_H_INCLUDED

COMMON(int) is_literal(term_t t);
COMMON(int) init_errors(void);
COMMON(int) permission_error(const char *op, const char *type, const char *obj,
			     const char *msg);
COMMON(int) get_prefixed_iri(rdf_db *db, term_t t, atom_t *a);
COMMON(void) flush_prefix_cache(void);

static inline int
get_iri_ex(rdf_db *db, term_t t, atom_t *a)
{ if ( PL_get_atom(t, a) )
    return TRUE;
  if ( get_prefixed_iri(db, t, a) )
    return TRUE;
  return PL_type_error("iri", t);
}


static inline int
get_atom_or_var_ex(term_t t, atom_t *a)
{ if ( PL_get_atom(t, a) )
    return TRUE;
  if ( PL_is_variable(t) )
  { *a = 0L;
    return TRUE;
  }

  return PL_type_error("atom", t);
}


static inline int
get_resource_or_var_ex(rdf_db *db, term_t t, atom_t *a)
{ if ( PL_get_atom(t, a) )
    return TRUE;
  if ( PL_is_variable(t) )
  { *a = 0L;
    return TRUE;
  }
  if ( get_prefixed_iri(db, t, a) )
    return TRUE;
  if ( is_literal(t) )
    return FALSE;			/* fail on rdf(literal(_), ...) */

  return PL_type_error("atom", t);
}


static inline int
get_bool_arg_ex(int a, term_t t, int *val)
{ term_t arg = PL_new_term_ref();

  if ( !PL_get_arg(a, t, arg) )
    return PL_type_error("compound", t);

  return PL_get_bool_ex(arg, val);
}


#endif /*ERROR_H_INCLUDED*/
