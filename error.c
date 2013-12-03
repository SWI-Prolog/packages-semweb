/*  Part of SWI-Prolog

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

#include "rdf_db.h"


		 /*******************************
		 *	       ERRORS		*
		 *******************************/

static functor_t FUNCTOR_error2;
static functor_t FUNCTOR_literal1;

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


#define MKFUNCTOR(n, a) \
	FUNCTOR_ ## n ## a = PL_new_functor(PL_new_atom(#n), a)

int
init_errors(void)
{ MKFUNCTOR(error, 2);
  MKFUNCTOR(literal, 1);

  return TRUE;
}
