# Assignment #3: Report

**Name:** Pablo Garces
**Student ID:** 801377963
**Email:** <pgarces@charlotte.edu>

---

## Seed and commands

Seed used for `datagen.py`: `801377963`

The commands you ran, in order:

```bash
python3 datagen.py 801377963
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output
# repeated the docker cp + spark-submit above after finishing each part
docker compose down
```

---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):

Instead of removing the bad rows directly, I first marked them so I could count them. For
rule 1, I added a true/false column that is true when the dropoff time is the same as or
earlier than the pickup time, since a valid trip has to end after it starts. For rule 2, I
converted the fare text into a number with `try_cast`, which gives an empty value instead of
an error when the text is not a number, so empty fares, `N/A`, `unknown` and `12,40` all end
up empty, and then marked those rows with a second true/false column. To get the counts I
added up how many rows were marked for each rule, and for `removed` I counted the rows marked
by either rule, so a row that breaks both rules is only removed once. The clean trips are the
rows that were not marked by any rule.

Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:

If `fare` were read as a `DOUBLE`, the CSV reader would have turned every bad fare into `null`
while reading, without any error. So `N/A`, `unknown` and `12,40` would look exactly the same as a
missing value, and there would be no way of knowing how many bad fares there were or what
they looked like. Reading it as a string keeps the original text, which gives more visibility and control over
what we want to do with values that are actually missing and values that have the wrong type.

The five counts:

| rows_total | bad_time | bad_fare | removed | rows_kept |
| ---------- | -------- | -------- | ------- | --------- |
| 100000     | 1519     | 1514     | 3033    | 96967     |

Two or three bad rows, as they appear in `trips.csv`:

```text
T000023,D028,4,3,2026-09-02 20:01:30,2026-09-02 19:56:24,1.63,7.43,cash
T000684,D044,13,8,2026-09-22 17:37:11,2026-09-22 17:55:23,3.21,N/A,app
T001398,D085,1,15,2026-09-19 23:31:45,2026-09-19 23:55:10,4.73,"12,40",cash
```

The first one is dropped off about five minutes before it was picked up (rule 1). The second
one has `N/A` as the fare and the third one has `12,40`, which uses a comma instead of a
decimal point (rule 2).

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```text
+----------+-------+---------------+-----+-------+
|day       |zone_id|zone_name      |trips|revenue|
+----------+-------+---------------+-----+-------+
|2026-09-01|1      |Uptown Center  |300  |5897.65|
|2026-09-01|2      |First Ward     |253  |4975.41|
|2026-09-01|3      |Fourth Ward    |247  |5001.5 |
|2026-09-01|4      |South End      |235  |4807.9 |
|2026-09-01|5      |Dilworth       |167  |3250.77|
|2026-09-01|6      |Plaza Midwood  |159  |3245.51|
|2026-09-01|7      |NoDa           |145  |2709.23|
|2026-09-01|8      |University City|179  |3327.29|
|2026-09-01|9      |Ballantyne     |107  |2133.85|
|2026-09-01|10     |SouthPark      |174  |3346.07|
+----------+-------+---------------+-----+-------+
```

We can see that the central zones like Uptown Center get more trips and revenue than the rest.
For example, on September 1st it had 300 trips while Ballantyne only had 107.

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```text
+---------+----------+-----+
|driver_id|day       |trips|
+---------+----------+-----+
|D001     |2026-09-01|23   |
|D002     |2026-09-12|26   |
|D003     |2026-09-10|22   |
|D004     |2026-09-13|18   |
|D005     |2026-09-24|20   |
|D006     |2026-09-05|21   |
|D007     |2026-09-03|19   |
|D008     |2026-09-01|20   |
|D009     |2026-09-21|21   |
|D010     |2026-09-18|21   |
+---------+----------+-----+
```

Every driver ends up with one busiest day of around 21 trips, and those peak days are spread
across the month.

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql
SELECT z.borough,
       COUNT(*)                     AS trips,
       ROUND(SUM(t.fare), 2)        AS revenue,
       ROUND(AVG(t.distance_km), 2) AS avg_distance_km
FROM trips t
JOIN zones z ON t.pickup_zone = z.zone_id
GROUP BY z.borough
ORDER BY revenue DESC
```

```text
+-------+-----+---------+---------------+
|borough|trips|revenue  |avg_distance_km|
+-------+-----+---------+---------------+
|Central|40887|804379.37|5.33           |
|South  |17017|337922.48|5.39           |
|North  |14766|294445.26|5.42           |
|East   |12396|244272.28|5.34           |
|West   |11901|235970.62|5.37           |
+-------+-----+---------+---------------+
```

Central has around 42% of all the trips and revenue, more than twice any other borough, while
the average distance is almost the same everywhere.

---

## The two plans

`explain()` output of part 2a (DataFrame API):

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [day#38 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(day#38 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=1547]
      +- HashAggregate(keys=[day#38, zone_id#9, zone_name#10], functions=[count(1), sum(fare#37)])
         +- Exchange hashpartitioning(day#38, zone_id#9, zone_name#10, 200), ENSURE_REQUIREMENTS, [plan_id=1544]
            +- HashAggregate(keys=[day#38, zone_id#9, zone_name#10], functions=[partial_count(1), partial_sum(fare#37)])
               +- Project [fare#37, day#38, zone_id#9, zone_name#10]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, try_cast(fare#7 as double) AS fare#37, cast(pickup_ts#4 as date) AS day#38]
                     :  +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1539]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,zone_name#10] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,zone_name:string>
```

`explain()` output of part 3 (Spark SQL):

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [revenue#188 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(revenue#188 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=1597]
      +- HashAggregate(keys=[borough#11], functions=[count(1), sum(fare#37), avg(distance_km#6)])
         +- Exchange hashpartitioning(borough#11, 200), ENSURE_REQUIREMENTS, [plan_id=1594]
            +- HashAggregate(keys=[borough#11], functions=[partial_count(1), partial_sum(fare#37), partial_avg(distance_km#6)])
               +- Project [distance_km#6, fare#37, borough#11]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, distance_km#6, try_cast(fare#7 as double) AS fare#37]
                     :  +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,distance_km#6,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,distance_km:double,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1589]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,borough#11] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,borough:string>
```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.

They look exactly the same. The file scans are at the bottom of each plan: each one has two
`FileScan csv`, one on `trips.csv` and one on `zones.csv`.

In both plans the join is a `BroadcastHashJoin ... Inner, BuildRight`. Since `zones.csv` only
has 25 rows, Spark decided to broadcast it, which means every executor gets its own copy of the
zones table (`BroadcastExchange`). Because of that, the trips stay where they are and are not
shuffled for the join.

Each plan has two `Exchange` operators. The `Exchange hashpartitioning` sits between the two
`HashAggregate` steps. Each executor only has part of the trips, so the rows that belong to the
same group have to be sent to the same executor before the aggregation can be finished. The
`Exchange rangepartitioning` sits right before the final `Sort`, since the rows have to be split
into ranges first so that the whole result can be sorted in order.

The two plans are the same operator by operator; the only differences are the grouping keys and
the extra `avg(distance_km)` in part 3. This is because the DataFrame API and Spark SQL are both
turned into the same physical plan by the Catalyst optimizer, so the language you write the
query in does not change how it runs.

---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.

I would keep the cleaning logic with `try_cast` and the broadcast join, becuase `zones` is always going to be 25 rows no matter how many trips.  The first thing I would change is
reading the CSV again and again. With 200 million rows I would convert it to Parquet once,
partitioned by `day`, and make parts 2 and 3 read `clean_trips` from there, so each query only reads the columns it needs.

---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

This time I learned from my previous mistakes. In earlier activities, `docker compose up -d`
failed because some ports were already taken by other processes, or because the cluster from
the previous activity was still running with the same container names. So before starting this
cluster, I freed the ports and stopped the containers from the last activity with
`docker compose down`, and `docker compose up -d` worked on the first try.

The only problem I had was with `duration_min`. Subtracting the two timestamp columns directly
does not give a number; it gives an interval, so dividing it by 60 does not give minutes. I had
to convert both timestamps to seconds with `unix_timestamp` first, and then divide the
difference by 60.

After that, the program finished with exit code 0, and the counts in part 1 and the totals in
part 3 match.
