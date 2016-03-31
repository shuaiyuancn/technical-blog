---
layout: post
title:  "Sampling in Spark: Methods"
date:   2016-03-31 16:56:52 +0100
categories: data_science spark
tags: spark pyspark etl
---

There are a few methods of sampling in Spark. 

### When you need a fraction

Call `rdd.sample(withReplacement, fraction, seed=None)`. 
This method is expensive because it uses `random.Random` class to generate random numbers. 
A random numder will be genenrated for each record.
See the [source code](https://github.com/apache/spark/blob/master/python/pyspark/rddsampler.py).

This method is also inconvenient when you don't know how many records are in the dataset.
You could estimate that by inspecting a single `part-*` file.

### When you need a fixed number of records

Call `rdd.takeSample(withReplacement, num, seed=None)`.
This method is even more expensive because it calls `rdd.sample` multiple times to increamentally get to the desired `num`:

~~~ python
while len(samples) < num:
  # TODO: add log warning for when more than one iteration was run
  seed = rand.randint(0, sys.maxsize)
  samples = self.sample(withReplacement, fraction, seed).collect()
~~~

See the [source code](https://github.com/apache/spark/blob/master/python/pyspark/rdd.py#L425).
Remember that `rdd.sample` will scan the dataset once --- `rdd.takeSample` could scan it multiple times.

### When you want speed more than randomness

If you are dealing with a truly large dataset, select by counting won't make too much difference from 
using random numbers _[citation needed]_.
In this case, you could use a sampling function like

~~~ python
def sample_a_partition(rows, one_out_of_n):
  count = 0
  for it in rows:
    count += 1
    if count == one_out_of_n:
      count = 0
      yield it
~~~

Note that the function needs applied to partitions like

~~~ python
from functools import partial

rdd.mapPartitions(
  partial(sample_a_partition, one_out_of_n=100)
)
~~~

Because it is impossible to save a global count when applying the function to individual records, like `rdd.map`.

I will compare the run time of three methods in Sampling in Spark: Run time comparison.

