aaa = LOAD 'input/udf.txt' USING udf.SampleLoadStorage() AS (T1:tuple(f1:chararray, f2:chararray), T2:tuple(f3:chararray, f4:chararray, T3:tuple(f5:chararray)));
DUMP aaa;