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

#include "rdf_db.h"

snapshot *
new_snapshot(rdf_db *db)
{ query *q = open_query(db);
  snapshot *ss = rdf_malloc(db, sizeof(*ss));

  ss->rd_gen = q->rd_gen;
  ss->tr_gen = q->tr_gen;
  ss->db = db;
  ss->symbol = 0;

  simpleMutexLock(&db->locks.misc);
  if ( db->snapshots.head )
  { ss->next = db->snapshots.head;
    db->snapshots.head->prev = ss;
    db->snapshots.head = ss;
    if ( ss->rd_gen < db->snapshots.keep )
      db->snapshots.keep = ss->rd_gen;
  } else
  { ss->next = ss->prev = NULL;
    db->snapshots.head = db->snapshots.tail = ss;
    db->snapshots.keep = ss->rd_gen;
  }
  simpleMutexUnlock(&db->locks.misc);

  close_query(q);

  return ss;
}


static void
free_snapshot(snapshot *ss)
{ rdf_db *db = ss->db;

  simpleMutexLock(&db->locks.misc);
  if ( ss->next )
    ss->next->prev = ss->prev;
  if ( ss->prev )
    ss->prev->next = ss->next;
  if ( ss == db->snapshots.head )
    db->snapshots.head = ss->next;
  if ( ss == db->snapshots.tail )
    db->snapshots.tail = ss->prev;

  if ( ss->rd_gen == db->snapshots.keep )
  { gen_t oldest = GEN_MAX;
    snapshot *s;

    for(s=db->snapshots.head; ss; ss=ss->next)
    { if ( s->rd_gen < oldest )
	oldest = s->rd_gen;
    }

    db->snapshots.keep = oldest;
  }
  simpleMutexUnlock(&db->locks.misc);

  rdf_free(db, ss, sizeof(*ss));
}


static void
acquire_snapshot(atom_t symbol)
{ snapshot *ss = PL_blob_data(symbol, NULL, NULL);
  ss->symbol = symbol;
}

static int
release_snapshot(atom_t symbol)
{ snapshot *ss = PL_blob_data(symbol, NULL, NULL);

  free_snapshot(ss);

  return TRUE;
}

static int
compare_snapshot(atom_t a, atom_t b)
{ snapshot *ssa = PL_blob_data(a, NULL, NULL);
  snapshot *ssb = PL_blob_data(b, NULL, NULL);

  return ( ssa->rd_gen > ssb->rd_gen ? 1 :
	   ssa->rd_gen < ssb->rd_gen ? -1 :
	   ssa->tr_gen > ssb->tr_gen ? 1 :
	   ssa->tr_gen < ssb->tr_gen ? -1 :
	   ssa > ssb ? 1 :
	   ssb < ssa ? -1 : 0
	 );
}

static int
write_snapshot(IOSTREAM *s, atom_t symbol, int flags)
{ snapshot *ss = PL_blob_data(symbol, NULL, NULL);

  Sfprintf(s, "<rdf-snapshot>(%ld)", (long)ss->rd_gen);
  return TRUE;
}

static PL_blob_t snap_blob =
{ PL_BLOB_MAGIC,
  PL_BLOB_NOCOPY|PL_BLOB_UNIQUE,
  "rdf_snapshot",
  release_snapshot,
  compare_snapshot,
  write_snapshot,
  acquire_snapshot
};


int
unify_snapshot(term_t t, snapshot *ss)
{ int rc = PL_unify_blob(t, ss, sizeof(*ss), &snap_blob);

  if ( !rc )
    free_snapshot(ss);

  return rc;
}


int
get_snapshot(term_t t, snapshot **ss)
{ PL_blob_t *type;
  void *data;

  if ( PL_get_blob(t, &data, NULL, &type) && type == &snap_blob)
  { *ss = data;

    return TRUE;
  }

  return FALSE;
}
