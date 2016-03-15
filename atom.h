/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2006-2016, University of Amsterdam
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

#ifndef ATOM_H_INCLUDED
#define ATOM_H_INCLUDED
#include <SWI-Stream.h>
#include <SWI-Prolog.h>
#include <wchar.h>

#define MAX_LIKE_CHOICES	100	/* max *'s in like pattern */

#define STR_MATCH_CASE		0x0	/* Default: perfect match */
#define STR_MATCH_PLAIN		0x1	/* Same, also match qualifier */
#define	STR_MATCH_ICASE		0x2	/* case-insensitive */
					/* keep after ICASE */
#define	STR_MATCH_SUBSTRING	0x3	/* substring */
#define	STR_MATCH_WORD		0x4	/* whole word */
#define	STR_MATCH_PREFIX	0x5	/* prefix */
#define STR_MATCH_LIKE		0x6	/* SeRQL *like* match */
					/* Keep after LIKE */
#define STR_MATCH_LT		0x7	/*  < */
#define STR_MATCH_LE		0x8	/* =< */
#define STR_MATCH_EQ		0x9	/* == */
#define STR_MATCH_GE		0xA	/* >= */
#define STR_MATCH_GT		0xB	/* >  */
#define STR_MATCH_BETWEEN	0xC	/* X .. Y */
					/* MAX: 0xf (4 bits in triple) */

typedef unsigned char charA;
typedef wchar_t       charW;

typedef struct text
{ const charA *a;
  const charW *w;
  size_t length;
} text;


typedef struct atom_info
{ atom_t	handle;
  text		text;
  int		resolved;
  int		rc;			/* TRUE if text atom */
} atom_info;


#ifdef COMPACT
typedef unsigned int atom_id;
#define ATOM_ID_SHIFT	7		/* Sync with SWI-Prolog */
#if PLVERSION >= 70101
#define TAG_ATOM	0x00000005L
#else
#define TAG_ATOM	0x00000004L
#endif

#define ATOM_ID(a)	((atom_id)(((uintptr_t)(a))>>ATOM_ID_SHIFT))
#define ID_ATOM(id)	(((uintptr_t)(id)<<ATOM_ID_SHIFT)|TAG_ATOM)
#else
typedef atom_t	atom_id;
#define ATOM_ID(a)	(a)
#define ID_ATOM(id)	(id)
#endif

COMMON(int)		cmp_atoms(atom_t a1, atom_t a2);
COMMON(int)		cmp_atom_info(atom_info *a1, atom_t a2);
COMMON(atom_t)		first_atom(atom_t a, int match);
COMMON(int)		match_atoms(int how, atom_t search, atom_t label);
COMMON(int)		match_text(int how, text *search, text *label);
COMMON(unsigned int)	atom_hash_case(atom_t a);
COMMON(int)		atom_lang_matches(atom_t lang, atom_t pattern);
COMMON(int)		fill_atom_info(atom_info *info);
COMMON(int)		fetch_atom_text(atom_t atom, text *txt);

#endif /*ATOM_H_INCLUDED*/
