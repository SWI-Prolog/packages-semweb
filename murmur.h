/*  $Id$

    Part of SWI-Prolog

    Author:	Austin Appleby
    License:	Public domain
    See:	http://murmurhash.googlepages.com/
*/

#ifndef PL_HASH_H_INCLUDED
#define PL_HASH_H_INCLUDED

#define	      MURMUR_SEED	(0x1a3be34a)
#define  SUBJ_MURMUR_SEED	(0x2161d395)
#define  PRED_MURMUR_SEED	(0x6b8ebc69)
#define   OBJ_MURMUR_SEED	(0x14e86b12)
#define	GRAPH_MURMUR_SEED	(0x78a64d55)

COMMON(unsigned int) rdf_murmer_hash(const void *key, int len, unsigned int seed);

static inline unsigned int
atom_hash(atom_t a, unsigned int seed)
{ return rdf_murmer_hash(&a, sizeof(a), seed);
}

#endif /*PL_HASH_H_INCLUDED*/
