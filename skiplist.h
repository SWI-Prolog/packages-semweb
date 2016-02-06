/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2012, VU University Amsterdam
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

#ifndef SKIPLIST_H_DEFINED
#define SKIPLIST_H_DEFINED

#ifndef SO_LOCAL
#ifdef HAVE_VISIBILITY_ATTRIBUTE
#define SO_LOCAL __attribute__((visibility("hidden")))
#else
#define SO_LOCAL
#endif
#define COMMON(type) SO_LOCAL type
#endif

#define SKIPCELL_MAX_HEIGHT 31
#define SKIPCELL_MAGIC	    2367357

typedef struct skipcell
{ unsigned	height : 6;		/* Max 63 height should do */
  unsigned	erased : 1;		/* Cell is erased */
  unsigned	magic : 25;		/* SKIPCELL_MAGIC */
  void	       *next[1];
} skipcell;


typedef struct skiplist
{ size_t	payload_size;		/* Size of payload */
  void	       *client_data;		/* Client data for call-backs */
  int		(*compare)(void *p1, void *p2, void *cd);
  void		(*destroy)(void *p, void *cd);
  void	       *(*alloc)(size_t bytes, void *cd);	/* Allocate a new cell */
  int		height;			/* highest cell */
  size_t	count;			/* #elements in skiplist */
  void	       *next[SKIPCELL_MAX_HEIGHT];
} skiplist;


typedef struct skiplist_enum
{ skipcell     *current;
  skiplist     *list;
} skiplist_enum;


void	skiplist_init(skiplist *sl, size_t payload_size,
		      void *client_data,
		      int  (*compare)(void*p1, void*p2, void*cd),
		      void*(*alloc)(size_t bytes, void *cd),
		      void (*destroy)(void*p, void *cd));
void   *skiplist_find(skiplist *sl, void *payload);
void   *skiplist_find_first(skiplist *sl, void *payload, skiplist_enum *en);
void   *skiplist_find_next(skiplist_enum *en);
void    skiplist_find_destroy(skiplist_enum *en);
void   *skiplist_insert(skiplist *sl, void *payload, int *is_new);
void   *skiplist_delete(skiplist *sl, void *payload);
void    skiplist_destroy(skiplist *sl);
int	skiplist_check(skiplist *sl, int print);
int	skiplist_debug(int new);
int	skiplist_erased_payload(skiplist *sl, void *payload);

#endif /*SKIPLIST_H_DEFINED*/
