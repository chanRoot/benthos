---
title: process_dag
type: processor
deprecated: true
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     lib/processor/process_dag.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::warning DEPRECATED
This component is deprecated and will be removed in the next major version release. Please consider moving onto [alternative components](#alternatives).
:::

A processor that manages a map of `process_map` processors and
calculates a Directed Acyclic Graph (DAG) of their dependencies by referring to
their postmap targets for provided fields and their premap targets for required
fields.

```yaml
# Config fields, showing default values
process_dag: {}
```

## Alternatives

All functionality of this processor has been superseded by the
[workflow](/docs/components/processors/workflow) processor.

The names of workflow stages may only contain alphanumeric, underscore and dash
characters (they must match the regular expression `[a-zA-Z0-9_-]+`).

The DAG is then used to execute the children in the necessary order with the
maximum parallelism possible. You can read more about workflows in Benthos
[in this document](/docs/configuration/workflows).

The field `dependencies` is an optional array of fields that a child
depends on. This is useful for when fields are required but don't appear within
a premap such as those used in conditions.

This processor is extremely useful for performing a complex mesh of enrichments
where network requests mean we desire maximum parallelism across those
enrichments.

## Examples

If we had three target HTTP services that we wished to enrich each
document with - foo, bar and baz - where baz relies on the result of both foo
and bar, we might express that relationship here like so:

``` yaml
process_dag:
  foo:
    premap:
      .: .
    processors:
    - http:
        url: http://foo/enrich
    postmap:
      foo_result: .

  bar:
    premap:
      .: msg.sub.path
    processors:
    - http:
        url: http://bar/enrich
    postmap:
      bar_result: .

  baz:
    premap:
      foo_obj: foo_result
      bar_obj: bar_result
    processors:
    - http:
        url: http://baz/enrich
    postmap:
      baz_obj: .
```

With this config the DAG would determine that the children foo and bar can be
executed in parallel, and once they are both finished we may proceed onto baz.

