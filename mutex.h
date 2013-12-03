/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        J.Wielemaker@cs.vu.nl
    WWW:           http://www.swi-prolog.org
    Copyright (C): 1985-2013, University of Amsterdam
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
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
    MA 02110-1301 USA
*/


/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
To allow for multiple thread-implementations, we  do not use plain POSIX
mutex-primitives in the remainder of  the   code.  Instead,  mutexes are
controlled using the following macros:

	type simpleMutex	Non-recursive mutex

	simpleMutexInit(p)	Initialise a simple mutex
	simpleMutexDelete(p)	Delete a simple mutex
	simpleMutexLock(p)	Lock a simple mutex
	simpleMutexTryLock(p)	Try Lock a simple mutex
	simpleMutexUnlock(p)	unlock a simple mutex

This file is a modified copy  of SWI-Prolog's pl-mutex.h, providing only
the simple mutexes.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

#ifndef MUTEX_H_DEFINED
#define MUTEX_H_DEFINED

#if defined(__WINDOWS__)
#define USE_CRITICAL_SECTIONS 1
#endif

#ifdef USE_CRITICAL_SECTIONS
#ifndef WINDOWS_LEAN_AND_MEAN
#define WINDOWS_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <windows.h>

#define simpleMutex CRITICAL_SECTION

#define simpleMutexInit(p)	InitializeCriticalSection(p)
#define simpleMutexDelete(p)	DeleteCriticalSection(p)
#define simpleMutexLock(p)	EnterCriticalSection(p)
#define simpleMutexUnlock(p)	LeaveCriticalSection(p)

#else /* USE_CRITICAL_SECTIONS */

#include <pthread.h>

typedef pthread_mutex_t simpleMutex;

#define simpleMutexInit(p)	pthread_mutex_init(p, NULL)
#define simpleMutexDelete(p)	pthread_mutex_destroy(p)
#define simpleMutexLock(p)	pthread_mutex_lock(p)
#define simpleMutexUnlock(p)	pthread_mutex_unlock(p)

#endif /*USE_CRITICAL_SECTIONS*/

#endif /*MUTEX_H_DEFINED*/

