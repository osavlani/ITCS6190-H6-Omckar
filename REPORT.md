# Hands-on L6: Report

**Name: Omckar Savlani**
**Student ID: 801497440**
**Email: osavlani@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`: 801497440

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
installed pyspark in .venv with pip install
# pip install pyspark, and suggested dialogue box implemented this as create new enviornment approach 

python datagen.py 801497440

docker compose -f docker-compose.codespaces.yml up -d
# used docker codespaces file

docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

docker exec spark-master curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080
# to check if port is actuall live and working, got response as 200, manually added ports in codespace still can't access ports 8080 and 4040. Visibility set to public and github says page not found (error 404) 

docker compose -f docker-compose.codespaces.yml down
# to shudown docker codespaces file
```

---

## Results

For each task, the first ten rows of your output (from the terminal or the CSV file) and one
or two sentences on what they say about your data.

### Task 1: favorite genre per user

```
user_id,genre,play_count
user_1,Pop,11
user_10,Rock,5
user_100,Pop,11
user_11,Classical,8
user_12,Rock,10
user_13,Hip-Hop,14
user_14,Jazz,5
user_15,Classical,12
user_16,Jazz,10
user_17,Pop,4
# Classical and hiphop had highest plau_counts but diversity between genres was well balanced.
```

### Task 2: average listening time per song

```
song_id,title,avg_duration_sec,play_count
song_31,Title_song_31,217.17,12
song_49,Title_song_49,206.18,11
song_11,Title_song_11,195.72,18
song_8,Title_song_8,194.5,24
song_24,Title_song_24,194.26,19
song_45,Title_song_45,191.36,14
song_33,Title_song_33,189.42,26
song_29,Title_song_29,186.06,17
song_38,Title_song_38,184.89,19
song_39,Title_song_39,183.52,23
# song 31 has the highest duration, song 27 has the most plays.
```

### Task 3: genre loyalty score, top 10

```
user_id,genre,play_count,total_plays,loyalty_score
user_66,Pop,10,10,1.0
user_39,Hip-Hop,7,7,1.0
user_45,Pop,5,5,1.0
user_15,Classical,12,13,0.923
user_100,Pop,11,12,0.917
user_4,Hip-Hop,11,12,0.917
user_79,Pop,10,11,0.909
user_26,Pop,9,10,0.9
user_94,Classical,9,10,0.9
user_95,Pop,9,10,0.9
# pop users show high loyalty, and rock genre is not to be spotted in top 10 loyalty users list.
```

Why do users with few plays tend to get a score of 1.0? Would you change the definition of
the score to account for that?
Yes as newcomers listening to a specific genre listen to more of that genre to start, and as pop is the most welcoming the data collected is inaccurate. We can keep a gate or a limit such as minium total_plays of 100 to be eligible for a loyalty score.

### Task 4: night owls

```
user_id,night_plays
user_1,6
user_4,6
user_56,6
user_93,6
user_21,5
user_28,5
user_17,4
user_47,4
user_49,4
user_55,4
# night_owls is an incomplete metric, it should be compared to how much they prefer to listen in day over night. Rather than recording who might sometimes listen a little during nights.
```

---

## The plan

Paste the `explain()` output of task 1:

```
AdaptiveSparkPlan isFinalPlan=false
+- Sort [user_id#0 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(user_id#0 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=975]
      +- Project [user_id#0, genre#7, play_count#28L]
         +- Filter (rank#38 = 1)
            +- Window [row_number() windowspecdefinition(user_id#0, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST, ...)] AS rank#38], [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST]
               +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Final
                  +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=968]
                        +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Partial
                           +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                              +- HashAggregate(keys=[user_id#0, genre#7], functions=[count(1)])
                                 +- Exchange hashpartitioning(user_id#0, genre#7, 200), ENSURE_REQUIREMENTS, [plan_id=962]
                                    +- HashAggregate(keys=[user_id#0, genre#7], functions=[partial_count(1)])
                                       +- Project [user_id#0, genre#7]
                                          +- BroadcastHashJoin [song_id#1], [song_id#4], Inner, BuildRight, false, false
                                             :- Filter isnotnull(song_id#1)
                                             :  +- FileScan csv [user_id#0,song_id#1] ... listening_logs.csv
                                             +- BroadcastExchange HashedRelationBroadcastMode(...)
                                                +- Filter isnotnull(song_id#4)
                                                   +- FileScan csv [song_id#4,genre#7] ... songs_metadata.csv
```

Your reading of it: where are the two file scans, which operator is the join and which kind
of join did Spark choose, where are the shuffles (`Exchange`) and why are they needed, and
how does this match the diagram in the SQL / DataFrame tab of the Spark UI?

The scans are at the bottom of both .csvs in input folder. 
The join spark choose was broadcast hash join.
The excahnge appear 3 times, 2 hashpartitionings (user_id, genre) and (user_id), and 1 rangepartitioning of (user_id).
Could'nt access UI tab.

---

## Transformations and actions

Which lines of your `main.py` are actions? How many jobs did the program launch according to
the Spark UI, and is that what you expected?

save() — df.show(20, truncate=False) and df.coalesce(1).write.mode("overwrite")...csv(...), are th main actions. 
Couldnt access UI, but I think the jobs weren't just 4 but way more as the task was large it had to be broken in many subtasks.

---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

Just the problem of accessing ports is still an issue.
