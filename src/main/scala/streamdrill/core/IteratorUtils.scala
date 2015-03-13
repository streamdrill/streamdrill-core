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
 * Iterator utils for intersecting and merging iterator results.
 *
 * @author Mikio L. Braun
 */
object IteratorUtils {
  def intersect[E](a: Iterator[E], b: Iterator[E])(implicit ord: Ordering[E]): Iterator[E] = {
    new IntersectingIterator[E](a, b)
  }

  def intersect[E](a: Iterable[E], b: Iterable[E])(implicit ord: Ordering[E]): Iterator[E] = {
    intersect(a.iterator, b.iterator)
  }

  def intersect[E](its: Seq[Iterator[E]])(implicit ord: Ordering[E]): Iterator[E] = {
    if (its.length == 0) {
      Seq().iterator
    } else if (its.length == 1) {
      its.head
    } else if (its.length == 2) {
      intersect(its.head, its.last)
    } else {
      intersect(its.head, merge(its.tail))
    }
  }

  def merge[E](s1: Iterable[E], s2: Iterable[E])(implicit ord: Ordering[E]): Iterator[E] = {
    merge(s1.iterator, s2.iterator)
  }

  def merge[E](it1: Iterator[E], it2: Iterator[E])(implicit ord: Ordering[E]): Iterator[E] = {
    new MergingIterator(it1, it2)
  }

  def merge[E](its: Seq[Iterator[E]])(implicit ord: Ordering[E]): Iterator[E] = {
    if (its.length == 0) {
      Seq().iterator
    } else if (its.length == 1) {
      its.head
    } else if (its.length == 2) {
      merge(its.head, its.last)
    } else {
      merge(its.head, merge(its.tail))
    }
  }
}
