/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 2002-2012, University of Amsterdam
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
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/

#ifndef SNAPSHOT_H_INCLUDED
#define SNAPSHOT_H_INCLUDED 1

typedef struct snapshot
{ struct snapshot *next;		/* Linked list of known snapshots */
  struct snapshot *prev;
  struct rdf_db	  *db;			/* Snapped database */
  gen_t		   rd_gen;		/* snapped generation */
  gen_t		   tr_gen;		/* snapped transaction generation */
  atom_t	   symbol;		/* Associated Prolog handle */
} snapshot;

#define SNAPSHOT_ANONYMOUS (snapshot*)1

COMMON(snapshot *)	new_snapshot(struct rdf_db *db);
COMMON(int)		unify_snapshot(term_t t, snapshot *ss);
COMMON(int)		get_snapshot(term_t t, snapshot **ss);
COMMON(int)		free_snapshot(snapshot *ss);
COMMON(void)		erase_snapshots(struct rdf_db *db);
COMMON(int)		snapshot_thread(snapshot *ss);

#endif /*SNAPSHOT_H_INCLUDED*/
