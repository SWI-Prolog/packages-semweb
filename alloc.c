/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2011 VU University Amsterdam

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

#include "rdf_db.h"
#include "deferfree.h"
#include "alloc.h"

triple *
alloc_triple(void)
{ triple *t = malloc(sizeof(*t));

  if ( t )
    memset(t, 0, sizeof(*t));

  return t;
}


void
unalloc_triple(rdf_db *db, triple *t, int linger)
{ if ( t )
  { assert(t->atoms_locked == FALSE);

    if ( linger )
      deferred_free(&db->defer_triples, t);
    else
      free(t);
  }
}


int
init_alloc(void)
{ return TRUE;
}
