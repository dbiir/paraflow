---
title: Introduction
---

# ParaFlow

## Why ParaFlow?
ID-associated data (sequences of entries, and each entry is semantically associated with a unique ID. ie. user IDs in user behaviour logs of mobile applications) contains valuable information for decision makings.
Timely and efficiently analyzing of ID-associated data can bring significant business value.
For example, by analyzing log data of servers and applications, we can infer root causes of failures.
By analyzing log data of e-commerce sites, we can learn recent changes in browsing and purchasing behaviours of specific customers.
Based on that, e-commerce sites can provide more personalized recommendations.

In above application scenarios, people need to perform real-time analyzing work on log data around some specific entities(customers, products, servers, applications etc.).
Entity centric analysis requires that the finest granularity of log data should be stored for later process.
In the meantime, real-time analyzing requires that the log data should be loaded into data warehouse as soon as possible.
In summary, two challenges remain un-tackled:
1. Data should be loaded as fast as possible without any loss.
2. Real-time queries should be executed as quickly as possible.

## Key Features
Paraflow enables users to load data into data warehouse (like HDFS) as soon as possible, and provides real-time analysis over data of being loaded and in the warehouse.
1. **Fast loading**. Paraflow utilizes a well-designed pipeline for efficient data loading.
2. **No loss staging**. Kafka is used in the system to stage data without losses.
3. **Real-time analysis**. Lightweight indices are used in Paraflow to speed up queries.

## Demonstration
[Demonstration Video](https://youtu.be/nMnFzUArOq4)

## Publication
+ Towards Real-time Analysis of ID-Associated Data. ER'18 Demo.
+ Entity Fiber based Partitioning, no Loss Staging and Fast Loading of Log Data. PDCAT'16.

## Contacting Us
For feedback and questions, feel free to email us:
+ Guodong Jin guod.jin@gmail.com
