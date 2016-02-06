/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2007-2012, University of Amsterdam
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

#ifndef HASH_H_INCLUDED
#define HASH_H_INCLUDED

typedef struct ptr_hash_node
{ struct ptr_hash_node *next;		/* next in chain */
  void *value;				/* hashed value */
} ptr_hash_node;


typedef struct ptr_hash_table
{ int entries;				/* # chains  */
  ptr_hash_node **chains;		/* hash chains */
} ptr_hash_table;

ptr_hash_table *new_ptr_hash(int entries);
void		destroy_ptr_hash(ptr_hash_table *hash);
int		add_ptr_hash(ptr_hash_table *hash, void *value);
int		for_ptr_hash(ptr_hash_table *hash,
			     int (*func)(ptr_hash_node *node, void *closure),
			     void *closure);

		 /*******************************
		 *	       ATOMS		*
		 *******************************/

typedef ptr_hash_table atom_hash_table;
#define new_atom_hash(entries) new_ptr_hash(entries, ATOM_HASH_SHIFT)
#define destroy_atom_hash(hash) destroy_ptr_hash(hash)
#define add_atom_hash(hash, atom) add_ptr_hash(hash, (void*)(atom))
#define for_atom_hash for_ptr_hash


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
SWI-Prolog note: Atoms are integers shifted by LMASK_BITS (7)
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#define POINTER_HASH_SHIFT 3

#endif /*HASH_H_INCLUDED*/
