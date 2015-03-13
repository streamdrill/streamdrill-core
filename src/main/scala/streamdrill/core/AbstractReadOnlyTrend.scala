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
 * Trait for a trend without updates.
 *
 * Just a trend where you get trends and scores for the entries it contains.
 *
 * @author Mikio L. Braun
 */
trait AbstractReadOnlyTrend[T] {
  /**
   * Size of trend
   */
  val capacity: Int

  /**
   * Time scale in milli seconds
   */
  val halfTimeInMs: Long

  /**
   * Size of the trend
   */
  def size: Int

  /**
   * Last time this trend was updated
   */
  var lastUpdate: Long

  /**
   * Get the score for element t at given timestamp
   */
  def score(t: T, timestamp: Long): Double

  /**
   * Get the score for element t at the last updated timestamp
   */
  def score(t: T): Double = score(t, lastUpdate)

  /**
   * Get a trend
   * @param count number of elements to return
   * @param offset number of elements to skip from the top
   * @return sequence of elements in descending order of activity
   */
  def query(count: Int, offset: Int = 0): Seq[T]

  /**
   * Get a trend, include scores directly
   * @param count number of elements to return
   * @param offset number of elements to skip from the top
   * @param timestamp timestamp at which scores should be aligned (if missing, use last update)
   * @return sequence of element/score pairs
   */
  def queryWithScore(count: Int, offset: Int = 0, timestamp: Long = 0L): Seq[(T, Double)]

  /**
   * Check whether trend contains a specific element
   */
  def contains(t: T): Boolean

  /**
   * Get smallest element of a trend
   */
  def smallest: T
}
