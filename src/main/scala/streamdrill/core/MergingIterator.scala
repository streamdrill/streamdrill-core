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

package streamdrill.core

/**
 * Merges two iterators
 *
 * Takes two iterators over ordered collections and merges them using an ordering.
 *
 * @author Mikio L. Braun
 */
class MergingIterator[E](i1: Iterator[E], i2: Iterator[E])(implicit ord: Ordering[E]) extends Iterator[E] {
  private val it1 = new LookaheadIterator(i1)
  private val it2 = new LookaheadIterator(i2)

  def hasNext: Boolean = it1.hasNext || it2.hasNext

  def next(): E = {
    if (!it1.hasNext) {
      it2.next()
    } else if (!it2.hasNext) {
      it1.next()
    } else {
      val c = ord.compare(it1.peek, it2.peek)
      if (c < 0) {
        it1.next()
      } else if (c > 0) {
        it2.next()
      } else {
        it1.next()
        it2.next()
      }
    }
  }
}
