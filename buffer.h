/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2011-2013, University of Amsterdam
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

#ifndef BUFFER_H_INCLUDED
#define BUFFER_H_INCLUDED

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Triples are buffered in an  array.  This   is  used  by transactions for
committing, etc. The data is   allocated using PL_malloc_uncollectable()
to make sure that discarded  triples  are   not  really  removed  by GC,
notably before their changes can be broadcasted.

FIXME: Check that all triple buffers are   allocated either on the stack
or in memory that is scanned  by   BDWGC.  The ones for transactions are
stored on the C-stack.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

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
    { triple **tmp = PL_malloc_uncollectable(TFAST_SIZE*2*sizeof(triple*));

      if ( tmp )
      { memcpy(tmp, b->base, (char*)b->top - (char*)b->base);
	b->base = tmp;
	b->max = b->base + TFAST_SIZE*2;
	b->top = b->base + TFAST_SIZE;
	*b->top++ = t;
      } else
	return FALSE;
    } else
    { size_t size = (b->max - b->base);
      triple **tmp = PL_malloc_uncollectable(size*2*sizeof(triple*));

      assert(b->top == b->max);

      if ( tmp )
      { memcpy(tmp, b->base, (char*)b->top - (char*)b->base);
	PL_free(b->base);
	b->base = tmp;
	b->top  = b->base + size;
	b->max  = b->base + size*2;
	*b->top++ = t;
      } else
	return FALSE;
    }
  }

  return TRUE;
}


static inline void
free_triple_buffer(triple_buffer *b)
{ if ( b->base && b->base != b->fast )
    PL_free(b->base);
}

#endif /*BUFFER_H_INCLUDED*/
