/*
 * Copyright (c) 2015, streamdrill UG (haftungsbeschränkt)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package streamdrill.util

import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

import streamdrill.logging.Logging

/**
 * Scala wrapper for a reentrant read/write lock.
 *
 * @author Mikio L. Braun
 */

class ReadWriteLock extends Logging {
  private val rwLock = new ReentrantReadWriteLock(true)

  private def withLock[E](l: Lock)(block: => E): E = {
    l.lock()
    try {
      block
    } finally {
      l.unlock()
    }
  }

  def readLock[E](block: => E): E = withLock[E](rwLock.readLock)(block)

  def writeLock[E](block: => E): E = withLock[E](rwLock.writeLock)(block)

  def acquireSharedLock() {
//    debug("Checking whether I have the write lock already")
//    if (rwLock.isWriteLockedByCurrentThread) {
//      error("Abort! Abort!")
//      throw new IllegalStateException("Thread already owns write lock. Shared lock would lead to deadlock")
//    }
//    debug("looks good, proceed")
    rwLock.readLock.lock()
  }

  def releaseSharedLock() { rwLock.readLock.unlock() }

  def acquireExclusiveLock() { rwLock.writeLock.lock() }

  def releaseExclusiveLock() { rwLock.writeLock.unlock() }
}

class NonLockingReadWriteLock extends ReadWriteLock {
  override def readLock[E](block: => E): E = block
  override def writeLock[E](block: => E): E = block
}
