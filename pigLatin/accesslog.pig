accesslogs = LOAD 'input/accesslog.txt' USING PigStorage() AS (url:chararray, name:chararray, sequence:int);
DUMP accesslogs;
grp = GROUP accesslogs BY url;
DUMP grp;
pv = FOREACH grp GENERATE group, COUNT(accesslogs.name);
DUMP pv;
uu = FOREACH grp {
  user = accesslogs.name;
  distinctUser = DISTINCT user;
  GENERATE group, COUNT(distinctUser);
}
DUMP uu;
seq = FOREACH grp {
  ord = ORDER accesslogs BY sequence;
  li = LIMIT ord 3;
  GENERATE FLATTEN(li);
}
DUMP seq;