/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2011, VU University Amsterdam

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

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
B+-tree implementation to store the literals.

See http://en.wikipedia.org/wiki/B%2B_tree
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#include "bptree.h"

int
bp_find(bp_tree *tree, bp_key target, bp_location *loc)
{ bp_node n;

  if ( (n=tree->root) )
  { for(;;)
    { int i = 0;
      int delta;

      while( i<c->key_count )
      { if ( (delta=(*tree->compare)(target, n->keys[i])) >= 0 )
	  i++;
	else
	  break;
      }

      if ( n->is_leaf )
      { return delta == 0 ? BP_FOUND : BP_FOUND_GREATER;
      } else
      { n = n->value[i].node;
      }
    }
  }

  return BP_NOTFOUND;
}
