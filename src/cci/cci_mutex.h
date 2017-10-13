/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

/*
 * cci_mutex.h
 *
 *  Created on: Nov 7, 2012
 *      Author: siwankim
 */

#ifndef CCI_MUTEX_H_
#define CCI_MUTEX_H_

#include "porting.h"

namespace cci
{
  class _Mutex
  {
  private:
    pthread_mutex_t mutex;

  public:
    _Mutex ()
    {
      pthread_mutex_init (&mutex, NULL);
    }

     ~_Mutex ()
    {
      pthread_mutex_destroy (&mutex);
    }

    int lock ()
    {
      return pthread_mutex_lock (&mutex);
    }

    int unlock ()
    {
      return pthread_mutex_unlock (&mutex);
    }
  };

  class _MutexAutolock
  {
  public:
    explicit _MutexAutolock(_Mutex *mutex) :
      mutex(mutex), is_unlocked(true)
    {
      mutex->lock();
    }

    virtual ~_MutexAutolock()
    {
      unlock();
    }

    void unlock()
    {
      if (is_unlocked)
        {
          is_unlocked = false;
          mutex->unlock();
        }
    }

  private:
    _Mutex *mutex;
    bool is_unlocked;

    _MutexAutolock(const _MutexAutolock &);
    void operator=(const _MutexAutolock &);
  };
}


#endif /* CCI_MUTEX_H_ */
