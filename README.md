# streamdrill-core library

Version 1.0 (initial public release)

This is the core streamdrill functionality. It contains the exponential decay
trend and some helper functions to store and load the data.

Further information on streamdrill can be found on https://streamdrill.com/

BUILDING
========

This is a standard maven project, so type ```mvn package``` to create the jar file.

In order to use the library in your project, add the following dependencies to
your pom.xml and install it into your local repository (```mvn install```):

```HTML
<dependency>
    <groupId>streamdrill</groupId>
    <artifactId>streamdrill-core</artifactId>
    <version>1.0</version>
</dependency>
```

streamdrill is written in Scala and currently uses version 2.11.2.

OVERVIEW
========

The main class in the package is ExpDecayTrend in streamdrill.core. It allows you
to accumulate counts over a given half time for millions of objects in parallel.
At construction, you specify the maximum number of objects you want to track at
in the trend. If there occur more elements, the least active ones are replaced in
the trend to make place for the new elements.

In addition, you can defined secondary indices, which allow you to drill down on the
trends to look for subtrends in your data.

Here is simple example which creates a trend, pushes some data into it and then
queries the trend:
```Scala
  val t = new ExpDecayTrend[Long](1000, 1000L)
  ...
  t.update(number, timestampe)
  ...
  val result = t.query(numberOfElements)
  ...
  val count = t.score(number)
```

For defining a secondary index, you have to define an IndexMap which maps an
entry to the value you want to index on.

See the "[examples](https://github.com/streamdrill/streamdrill-core/tree/master/examples)" directory for Scala scripts.

LICENSE
=======

Copyright (c) 2015, streamdrill UG (haftungsbeschränkt)
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
