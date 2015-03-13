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
 * Routines for dealing with exponential decay numbers.
 *
 * After some experimentation, I've settled on the following format:
 *
 * 52 bits used to represent to exponent with 47 bits for the fractional part, 5 bits for
 * the integer part.
 *
 * This results in a range of roughly up to 2 ** 32 at which you still have a resolution of about 2e-5.
 * At that precision, a millisecond can still be resolved of half times up to 4400 years.

 * Format:
 *
 * 64     global sign  = s
 * 63     positive sign of factor
 * 62-53  -- unused --
 * 53     sign for the exponent = c
 * 52- 1  52 bits for the exponent =
 *
 * @author Mikio L. Braun
 */
object ExpDecay {
  val maxBits = 52
  val base = 47
  private val b = 1L << base

  private val SIGN_MASK = 1L << 63
  private val NOT_ZERO_MASK = 1L << 62
  private val COUNT_MASK = (1L << maxBits) - 1
  private val COUNT_SIGN_MASK = 1L << (maxBits + 1)

  val MAX_VALUE = COUNT_MASK | NOT_ZERO_MASK
  val MIN_RESOLUTION = toDouble(MAX_VALUE) - toDouble(MAX_VALUE - 1)
  val MAX_E = count(MAX_VALUE).toDouble / b

  /**
   * Get the sign part of an ExpDecay number
   */
  def sign(ed: Long) = if ((ed & SIGN_MASK) != 0) -1 else if ((ed & NOT_ZERO_MASK) != 0) 1 else 0

  /**
   * Get the count part of an ExpDecay number (that is, the exponent)
   */
  def count(ed: Long) = if ((ed & COUNT_SIGN_MASK) != 0)
    -(ed & COUNT_MASK)
  else
    ed & COUNT_MASK

  private def makeSign(sign: Long) = if (sign < 0)
    SIGN_MASK
  else if (sign > 0)
    NOT_ZERO_MASK
  else
    0L

  private def makeCount(count: Long) = if (count < 0) {
    COUNT_SIGN_MASK | ((-count) & COUNT_MASK)
  } else {
    count & COUNT_MASK
  }

  /**
   * Create an ExpDecay number
   * @param sign +1 or -1
   * @param count count (exponent) of the ExpDecay number
   * @return ExpDecay number encoded in a Long
   */
  def make(sign: Long, count: Long): Long = makeSign(sign) | makeCount(count)

  /**
   * Make an ExpDecay number using the supplied double as exponent
   * @param f number will be 2&#94;f
   * @return ExpDecay number encoded in a Long
   */
  def makeExp(f: Double) =
  if (f >= 32.0)
    throw new IllegalArgumentException("number too large!")
  else if (f <= -32.0)
    make(0)
  else
    make(1L, (f * b).toLong)

  /**
   * turn a double number into a fixed point number using the number
   * of bits after the "dot"
   * @param f
   * @return
   */
  def makeFixed(f: Double) = (f * b).toLong

  /**
   * Compute a Double from an ExpDecay number
   * @param ed
   * @return
   */
  def toDouble(ed: Long) = sign(ed) * math.pow(2.0, count(ed).toDouble / b)

  private val LOG2 = math.log(2.0)

  private def log2(x: Double) = math.log(x) / LOG2

  /**
   * Make an ExpDecay number to represent the given Double
   * @param f
   * @return
   */
  def make(f: Double): Long =
    if (f < 0)
      make(-1, (log2(-f) * b).toLong)
    else if (f > 0)
      make(1, (log2(f) * b).toLong)
    else
      make(0, 0)

  /**
   * Multiply two ExpDecay numbers
   * @param e1
   * @param e2
   * @return
   */
  def mult(e1: Long, e2: Long) =
    make(sign(e1) * sign(e2), count(e1) + count(e2))

  /**
   * Compare two long functions (redefined here for performance)
   * @param a
   * @param b
   * @return
   */
  def cmpLong(a: Long, b: Long) = if (a < b) -1 else if (a > b) 1 else 0

  /**
   * Compare two ExpDecay numbers
   * @param e1 ExpDecay number encoded in Long
   * @param e2 ExpDecay number encoded in Long
   * @return +1, 0, -1 depending on e1 <, ==, > e2
   */
  def cmp(e1: Long, e2: Long) = {
    val s1 = sign(e1)
    val s2 = sign(e2)
    val c1 = count(e1)
    val c2 = count(e2)
    if (s1 == 1 && s2 == 1) {
      cmpLong(c1, c2)
    } else if (s1 == -1 && s2 == -1) {
      cmpLong(c2, c1)
    } else if (s1 == -1 && s2 == 1) {
      -1
    } else if (s1 == 1 && s2 == -1) {
      1
    } else {
      0
    }
  }

  /**
   * Compare two ExpDecay numbers with shift
   *
   * The "shift" is a factor which is added to the exponent (which would be
   * an addition in the original representation). This is useful when comparing
   * two ExpDecay numbers rescaled to different times.
   *
   * @param e1 ExpDecay number encoded in Long
   * @param e2 ExpDecay number encoded in Long
   * @param shift number to add to e1
   * @return +1, 0, -1 depending on e1 + shift <, ==, > e2
   */
  def cmpWithShift(e1: Long, e2: Long, shift: Long): Int = {
    val s1 = sign(e1)
    val s2 = sign(e2)
    val c1 = count(e1)
    val c2 = count(e2)
    if (s1 == 1 && s2 == 1) {
      cmpLong(c1 + shift, c2)
    } else if (s1 == -1 && s2 == -1) {
      cmpLong(c2, c1 + shift)
    } else if (s1 == -1 && s2 == 1) {
      -1
    } else if (s1 == 1 && s2 == -1) {
      1
    } else if (s1 == 0) {
      -math.signum(s2)
    } else if (s2 == 0) {
      math.signum(s1)
    } else {
      0
    }
  }

  private def cmpPositiveWithShift(c1: Long, c2: Long, shift: Double): Int = {
    java.lang.Double.compare(c1.toDouble / b + shift, c2.toDouble / b)
  }

  /**
   * Same as cmpWithShift, but shift is encoded in Double
   *
   * @param e1 ExpDecay number encoded in Long
   * @param e2 ExpDecay number encoded in Long
   * @param shift number to add to e1 in the exponent
   * @return
   */
  def cmpWithShift(e1: Long, e2: Long, shift: Double): Int = {
    val s1 = sign(e1)
    val s2 = sign(e2)
    val c1 = count(e1)
    val c2 = count(e2)
    if (s1 == 1 && s2 == 1) {
      cmpPositiveWithShift(c1, c2, shift)
    } else if (s1 == -1 && s2 == -1) {
      cmpPositiveWithShift(c2, c1, -shift)
    } else {
      // s1 and s2 will be different or 0
      cmpLong(s1, s2)
    }
  }

  private def cmpPositiveWithShift(c1: Long, c2: Long, shift: Long, shiftBits: Int): Int = {
    //println("c1=%016x c2=%016x shift=%016x shiftBits=%d trueShift=%d shifted shift=%016x".format(c1, c2, shift, shiftBits, base - shiftBits, shift << (base - shiftBits)))
    cmpLongWithShift(c1, shift, base - shiftBits, c2)
  }

  /**
   * check whether s << shift would lead to an overflow or not.
   *
   * @param s
   * @param shift
   * @return
   */
  private def shiftWouldOverflow(s: Long, shift: Int): Boolean = {
    s != (s << shift >> shift)
  }

  /**
   * Compare long numbers c1 + (s << sShift) and c2, with s shifted by sShift bits to the left,
   * where c1 + s might potentially lead to a numeric overflow. So instead, we
   * treat the cases separately. And make sure we don't add up large numbers.
   *
   * We distinguish the cases where s > 0 and s < 0.
   *
   * If s > 0:
   * if c1 > c2, we're already done, because adding s can make c1 only larger
   * => 1
   * else we need check whether c1 + s > c2. Instead we check c2 - c1 against s
   * (because c1 + s vs. c2 is equiv to s vs. c2 - c1).
   *
   * Note that c2 - c1 can be at most twice the range of integers, but since
   * we assume that they come from expdecay numbers, everything is safe (hopefully)
   *
   * If s < 0:
   * the other way round (that is, c2 > c1 -> -1, else check c1 - c2 against -s
   *
   * @param c1 at most maxBits worth of long value
   * @param s any long number
   * @param sShift how many bits s is shifted to the left.
   * @param c2 at most maxBits worht of long value
   * @return -1, 0, 1 depending on the comparison of c1 + (s << sShift) and c2
   */
  private def cmpLongWithShift(c1: Long, s: Long, sShift: Int, c2: Long): Int = {
    if (shiftWouldOverflow(s, sShift)) {
      //println("Would overflow!")
      cmpLong(s, 0)
    } else {
      if (s > 0) {
        if (c1 > c2) {
          1
        } else {
          // here, c1 < c2, so c2 - c1 will be at most 2 * MAX_LONG
          //println("Comparing %d against %d".format(s << sShift, c2 - c1))
          cmpLong(s << sShift, c2 - c1)
        }
      } else { // s <= 0
        if (c2 > c1) {
          -1
        } else { // c1 < c2
          //println("Comparing %d against %d".format(c1 - c2, -s << sShift))
          cmpLong(c1 - c2, -s << sShift)
        }
      }
    }
  }

    def cmpWithShift(e1: Long, e2: Long, shift: Long, shiftBits: Int): Int = {
      val s1 = sign(e1)
      val s2 = sign(e2)
      val c1 = count(e1)
      val c2 = count(e2)
      if (s1 == 1 && s2 == 1) {
        cmpPositiveWithShift(c1, c2, shift, shiftBits)
      } else if (s1 == -1 && s2 == -1) {
        cmpPositiveWithShift(c2, c1, -shift, shiftBits)
      } else {
        // s1 and s2 will be different or 0
        cmpLong(s1, s2)
      }
    }

    /**
     * Add two ExpDecay numbers.
     *
     * This involves floating point arithemetic and is not stable numerically (if you do this a lot,
     * errors add up)
     *
     * @param ed ExpDecay number encoded in Long
     * @param f Double to add
     * @return new ExpDecay number representing the sum of the two
     */
    def add(ed: Long, f: Double) = make(toDouble(ed) + f)

    /**
     * Dump an ExpDecay number to the console.
     *
     * Print the different parts, mostly useful for
     *
     * @param ed
     * @return
     */
    def dump(ed: Long) = (if ((ed & SIGN_MASK) != 0) "-" else "") +
        (if ((ed & NOT_ZERO_MASK) != 0) "+" else "") +
        "2^" +
        (if ((ed & COUNT_SIGN_MASK) != 0) "-" else "") +
        "%x".format((ed & COUNT_MASK) / b) + "." +
        "%x".format((ed & COUNT_MASK) - (((ed & COUNT_MASK) >> base) << base))

    /**
     * Some self-tests
     *
     * Sorry about this ;)
     *
     * @param args
     */
    def main(args: Array[String]) {
      println(dump(MAX_VALUE))

      println("MAX_VALUE = " + toDouble(MAX_VALUE))

      println("MIN_RESOLUTION = " + MIN_RESOLUTION)

      println("MAX_VALUE = " + toDouble(MAX_VALUE | SIGN_MASK))

      var a = make(1.0)

      println("%f (%s)".format(toDouble(a), dump(a)))

      a = add(a, 1.0)
      println("%f (%s)".format(toDouble(a), dump(a)))

      a = add(a, 1.0)
      println("%f (%s)".format(toDouble(a), dump(a)))

      a = add(a, 1.0)
      println("%f (%s)".format(toDouble(a), dump(a)))

      a = add(a, 1.0)
      println("%f (%s)".format(toDouble(a), dump(a)))

      a = add(a, -10.0)
      println("%f (%s)".format(toDouble(a), dump(a)))

      println("%f (%s)".format(toDouble(mult(a, a)), dump(mult(a, a))))

      println("%f (%s)".format(toDouble(mult(a, mult(a, a))), dump(mult(a, mult(a, a)))))

      for (k <- 1 to 10) {
        runSome(10000000)
      }
    }

    /** Run some speed tests. */
    def runSome(n: Int) {
      var a = make(1.0)
      val savedTime = System.nanoTime
      var i = 0
      while (i < n) {
        a = add(a, 1.0)
        i += 1
      }
      val elapsed = (System.nanoTime - savedTime) / 1e9
      println("%d iters in %.1f (%.1f per sec)".format(n, elapsed, n / elapsed))
    }
  }
