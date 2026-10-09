   -- Day 5: slowed the replay so it lasts ~9 more days (until the Databricks trial ends ~Oct 19).
   -- 36 ticks/day (every 15 min, 10:00-18:59 IST); days left = remaining hours / (step * 36).
   -- Applied on 2026-10-09 with psql. Changed step from 80 hours to 20 hours.
   UPDATE etl.replay_state SET step = interval '20 hours' WHERE id = 1;
   SELECT sim_ts, step, end_ts FROM etl.replay_state;
