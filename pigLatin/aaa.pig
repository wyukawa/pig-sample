aaa = LOAD 'input/aaa.txt' USING PigStorage() AS (name:chararray, age:int);
DUMP aaa;