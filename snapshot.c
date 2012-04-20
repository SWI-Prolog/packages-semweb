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

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Create a new snapshot, adding it  as   the  _start_ of the DB's snapshot
list.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

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
    ss->prev = NULL;
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
unlink_snapshot(snapshot *ss)
{ rdf_db *db = ss->db;

  if ( ss->next )
    ss->next->prev = ss->prev;
  if ( ss->prev )
    ss->prev->next = ss->next;
  if ( ss == db->snapshots.head )
    db->snapshots.head = ss->next;
  if ( ss == db->snapshots.tail )
    db->snapshots.tail = ss->prev;
}


static void
update_keep_snapshot(snapshot *ss)
{ rdf_db *db = ss->db;

  if ( ss->rd_gen == db->snapshots.keep )
  { gen_t oldest = GEN_MAX;
    snapshot *s;

    for(s=db->snapshots.head; s; s=s->next)
    { if ( s->rd_gen < oldest )
	oldest = s->rd_gen;
    }

    db->snapshots.keep = oldest;
    DEBUG(1, { char buf[64];
	       Sdprintf("Deleted oldest snapshot; set keep gen to %s\n",
			gen_name(oldest, buf));
	     });
  }
}


int
free_snapshot(snapshot *ss)
{ rdf_db *db = ss->db;
  int rc;

  simpleMutexLock(&db->locks.misc);
  if ( (rc=(ss->symbol != 0)) )
  { unlink_snapshot(ss);
    update_keep_snapshot(ss);
    ss->symbol = 0;
  }
  simpleMutexUnlock(&db->locks.misc);

  return rc;
}


void
erase_snapshots(rdf_db *db)
{ snapshot *ss;

  simpleMutexLock(&db->locks.misc);
  while( (ss=db->snapshots.head) )
  { unlink_snapshot(ss);
    ss->symbol = 0;
  }
  db->snapshots.keep = GEN_MAX;
  simpleMutexUnlock(&db->locks.misc);
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
  rdf_free(ss->db, ss, sizeof(*ss));

  return TRUE;
}

static int
compare_snapshot(atom_t a, atom_t b)
{ snapshot *ssa = PL_blob_data(a, NULL, NULL);
  snapshot *ssb = PL_blob_data(b, NULL, NULL);

  return ( ssa->rd_gen > ssb->rd_gen ?  1 :
	   ssa->rd_gen < ssb->rd_gen ? -1 :
	   ssa->tr_gen > ssb->tr_gen ?  1 :
	   ssa->tr_gen < ssb->tr_gen ? -1 :
	   ssa > ssb ?  1 :
	   ssb < ssa ? -1 : 0
	 );
}

static int
write_snapshot(IOSTREAM *s, atom_t symbol, int flags)
{ snapshot *ss = PL_blob_data(symbol, NULL, NULL);
  char buf[64];

  if ( ss->tr_gen > GEN_TBASE )
  { char buf2[64];
    Sfprintf(s, "<rdf-snapshot>(%s+%s)",
	     gen_name(ss->rd_gen, buf),
	     gen_name(ss->tr_gen, buf2));
  } else
  { Sfprintf(s, "<rdf-snapshot>(%s)", gen_name(ss->rd_gen, buf));
  }

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
get_snapshot(term_t t, snapshot **ssp)
{ PL_blob_t *type;
  void *data;

  if ( PL_get_blob(t, &data, NULL, &type) && type == &snap_blob)
  { snapshot *ss = data;

    if ( ss->symbol )
    { *ssp = ss;

      return TRUE;
    }

    return -1;
  }

  return FALSE;
}


/* snapshot_thread() is the id of the thread that created the snapshot
   if the snapshot is created inside a modified transaction.
*/

int
snapshot_thread(snapshot *ss)
{ if ( ss->tr_gen > GEN_TBASE &&
       ((ss->tr_gen-GEN_TBASE)%GEN_TNEST) != 0 )
    return (ss->tr_gen-GEN_TBASE)/GEN_TNEST;

  return 0;
}
