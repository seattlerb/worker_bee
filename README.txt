= worker_bee

home :: http://seattlerb.org/

== DESCRIPTION:

WorkerBee encapsulates a simple pipeline of workers.

== FEATURES/PROBLEMS:

* Simple API to wrap up the usual Thread/Queue patterns.
* Expressive shortcuts to make the pipeline really easy to read.

== SYNOPSIS:

    bee = WorkerBee.new
    bee.input enum_of_work_to_do
    bee.work(20) { |work| ... stuff with input ... }
    bee.work(5)  { |work| ... stuff with results of previous ... }
    bee.results # the final set of results

Example of shortcuts:

    bee = WorkerBee.new
    bee.input(*urls)
    bee.work     { |url| url_to_api    url } # -> api_url
    bee.work(20) { |url| url_to_ary    url } # -> [ issues_or_pulls ]
    bee.work     { |ary| issues_to_url ary } # -> url_to_check
    bee.compact                              # -> url
    bee.results.sort

versus:

    WorkerBee.new(self) # self = receiver of send for `then`s below
      .input(*urls)
      .then(:url_to_api)
      .then(:url_to_ary, n:20)
      .then(:issues_to_url)
      .compact
      .results
      .sort

== REQUIREMENTS:

* ruby... awesome, no?

== INSTALL:

* sudo gem install worker_bee

== LICENSE:

(The MIT License)

Copyright (c) Ryan Davis, seattle.rb

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
