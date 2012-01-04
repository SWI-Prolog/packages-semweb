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

#ifndef RDF_MEMORY_H_INCLUDED
#define RDF_MEMORY_H_INCLUDED

/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Stuff for lock-free primitives

  * MSB(unsigned int i)
  Computes the most-significant bit, which often translates to
  a single machine operation.  Returns 0 if i=0;

     MSB(0) = undefined
     MSB(1) = 0
     MSB(2) = 1
     ...

  * MemoryBarrier()
  Realises a (full) memory barrier.  This means that memory operations
  before the barrier are all executed before those after the barrier.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#ifdef _MSC_VER				/* Windows MSVC version */

static inline int
MSB(unsigned int i)
{ unsigned long mask = i;
  unsigned long index;

  _BitScanReverse(&index, mask);	/* 0 if mask is 0 */
  return index;
}

#ifndef MemoryBarrier
#define MemoryBarrier() (void)0
#endif

#elif defined(__GNUC__)			/* GCC version */

#define MSB(i) (assert(i), 31 - __builtin_clz(i))
#define MemoryBarrier() __sync_synchronize()

#else					/* Other */

static inline int
MSB(unsigned int i)
{ int j = 0;

  if (i >= 0x10000) {i >>= 16; j += 16;}
  if (i >=   0x100) {i >>=  8; j +=  8;}
  if (i >=    0x10) {i >>=  4; j +=  4;}
  if (i >=     0x4) {i >>=  2; j +=  2;}
  if (i >=     0x2) j++;

  return j;
}
#define MemoryBarrier() (void)0

#endif



#endif /*RDF_MEMORY_H_INCLUDED*/
