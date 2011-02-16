/*  $Id$

    Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2002-2010, University of Amsterdam
			      VU University Amsterdam

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

#ifndef BUFFER_H_INCLUDED
#define BUFFER_H_INCLUDED

#define TFAST_SIZE 64

typedef struct triple_buffer
{ triple **base;
  triple **top;
  triple **max;
  triple  *fast[TFAST_SIZE];
} triple_buffer;


static inline void
init_triple_buffer(triple_buffer *b)
{ b->base = b->top = b->fast;
  b->max = b->top + TFAST_SIZE;
}


static inline int
is_empty_buffer(triple_buffer *b)
{ return b->top == b->base;
}


static inline int
buffer_triple(triple_buffer *b, triple *t)
{ if ( b->top < b->max )
  { *b->top++ = t;
  } else
  { if ( b->base == b->fast )
    { triple **tmp = malloc(TFAST_SIZE*2*sizeof(triple*));

      if ( tmp )
      { memcpy(tmp, b->base, (char*)b->top - (char*)b->base);
	b->base = tmp;
	b->max = b->base + TFAST_SIZE*2;
	b->top = b->base + TFAST_SIZE;
	*b->top++ = t;
      } else
	return FALSE;
    } else
    { triple **tmp = realloc(b->base, ((char*)b->max-(char*)b->base)*2);

      if ( tmp )
      { b->max += tmp-b->base;
	b->top += tmp-b->base;
	b->base = tmp;
	*b->top++ = t;
      } else
	return FALSE;
    }
  }

  return TRUE;
}


static inline void
free_triple_buffer(triple_buffer *b)
{ if ( b->base != b->fast )
    free(b->base);
}

#endif /*BUFFER_H_INCLUDED*/
