---
title: Keyed Row Selection
sidebar_label: Keyed row selection
---

TODO: summary

```groovy
notKeyed = emptyTable(100).update("Key1=i%3", "Key2=(i+1)%3", "Value=i")
keyedTable = notKeyed.withKeys("Key1", "Key2")
```

![Keyed row selection](../assets/how-to/keyed-row-selection.png)

```groovy
notKeyed = emptyTable(100).update("Key1=i", "Key2=i+1", "Value=i*2")
uniqueKeyedTable = notKeyed.withUniqueKeys("Key1", "Key2")
```

![Unique keyed row selection](../assets/how-to/keyed-row-selection-unique.png)