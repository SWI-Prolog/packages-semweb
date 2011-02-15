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
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef ERROR_H_INCLUDED
#define ERROR_H_INCLUDED

COMMON(int)	instantiation_error(term_t actual);
COMMON(int)	type_error(term_t actual, const char *expected);
COMMON(int)	domain_error(term_t actual, const char *expected);
COMMON(int)	permission_error(const char *op, const char *type,
				 const char *obj, const char *msg);
COMMON(int)	is_literal(term_t t);
COMMON(int)	init_errors(void);

static inline int
get_atom_ex(term_t t, atom_t *a)
{ if ( PL_get_atom(t, a) )
    return TRUE;

  return type_error(t, "atom");
}


static inline int
get_long_ex(term_t t, long *v)
{ if ( PL_get_long(t, v) )
    return TRUE;

  return type_error(t, "integer");
}


static inline int
get_double_ex(term_t t, double *v)
{ if ( PL_get_float(t, v) )
    return TRUE;

  return type_error(t, "float");
}


static inline int
get_atom_or_var_ex(term_t t, atom_t *a)
{ if ( PL_get_atom(t, a) )
    return TRUE;
  if ( PL_is_variable(t) )
  { *a = 0L;
    return TRUE;
  }

  return type_error(t, "atom");
}


static inline int
get_resource_or_var_ex(term_t t, atom_t *a)
{ if ( PL_get_atom(t, a) )
    return TRUE;
  if ( PL_is_variable(t) )
  { *a = 0L;
    return TRUE;
  }
  if ( is_literal(t) )
    return FALSE;			/* fail on rdf(literal(_), ...) */

  return type_error(t, "atom");
}


static inline int
get_bool_arg_ex(int a, term_t t, int *val)
{ term_t arg = PL_new_term_ref();

  if ( !PL_get_arg(a, t, arg) )
    return type_error(t, "compound");
  if ( !PL_get_bool(arg, val) )
    return type_error(arg, "bool");

  return TRUE;
}


#endif /*ERROR_H_INCLUDED*/
