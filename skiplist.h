/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2011, VU University Amsterdam

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

    As a special exception, if you link this library with other files,
    compiled with a Free Software compiler, to produce an executable, this
    library does not by itself cause the resulting executable to be covered
    by the GNU General Public License. This exception does not however
    invalidate any other reasons why the executable file might be covered by
    the GNU General Public License.
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
