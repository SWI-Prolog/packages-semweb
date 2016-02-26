/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2016, VU University Amsterdam
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

#ifndef XSD_H_INCLUDED
#define XSD_H_INCLUDED

#include <SWI-Prolog.h>

#ifndef SO_LOCAL
#ifdef HAVE_VISIBILITY_ATTRIBUTE
#define SO_LOCAL __attribute__((visibility("hidden")))
#else
#define SO_LOCAL
#endif
#define COMMON(type) SO_LOCAL type
#endif

#include "atom.h"

#define URL_xsd		  "http://www.w3.org/2001/XMLSchema#"
#define URL_xsdString     URL_xsd "string"
#define URL_xsdDouble     URL_xsd "double"

typedef enum xsd_primary
{ XSD_NONNUMERIC = 0,
  XSD_INTEGER,
  XSD_DECIMAL,
  XSD_DOUBLE
} xsd_primary;

typedef struct xsd_type
{ const char   *url;			/* URL */
  atom_t	url_atom;		/* As an atom */
  xsd_primary	primary;		/* primary type */
  int64_t	min_value;		/* min for restricted integers */
  int64_t	max_value;		/* max for restricted integers */
} xsd_type;

COMMON(xsd_primary)	is_numeric_type(atom_t type);
COMMON(int)		xsd_compare_numeric(
			    xsd_primary type1, const unsigned char *s1,
			    xsd_primary type2, const unsigned char *s2);
COMMON(int)		cmp_xsd_info(xsd_primary type1, atom_info *v1,
				     xsd_primary type2, atom_t v2);
#endif /*XSD_H_INCLUDED*/
