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

#ifndef BPTREE_H_INCLUDED
#define BPTREE_H_INCLUDED

#define BP_ORDER 5			/* Number of keys per node */

typedef void    *bp_key;		/* Key is a pointer to a literal */
typedef void    *bp_value;

typedef union
{ struct bp_node *node;
  bp_value	  value;
} bp_ptr;

typedef struct bp_node
{ int	   key_count;			/* # keys in node */
  int	   is_leaf;			/* This is a lef-node */
  bp_key   keys[BP_ORDER];		/* The node-array */
  bp_ptr   values[BP_ORDER];		/* associated values */
  struct   bp_node parent;		/* Parent node */
  struct   bp_node next;		/* Next for walking */
} bp_noleaf;

typedef bp_location
{ bp_node *node;			/* Note where we found the key */
  int	   offset;			/* Offset at which the key is */
} bp_location;

typedef struct bp_tree
{ bp_node *root;
  size_t   size;
  int	  (*compare)(bp_key k1, bp_key k2);
} bp_tree;


#endif /*BPTREE_H_INCLUDED*/
