# pyspark-geo-anlaysis
Repo for all codes to compute OD matrix from mobile data set of Shanghai Unicom subscribers, which contains 2.7billion records.
Please refer to the paper "Optimal policies weighing between the health cost and economic cost in response to the COVID-19" for the details.

## Requirement
1. Spark cluster and pyspark 2.0+
2. python-geohash
3. pandas, scipy, cython

## Installation
1. The main (master) node of Spark cluster should have at least 64GB RAM for running the script, as in the final step, the script will allocate all data to the main machine for computing the OD matrix.
2. Please refer to the Spark documentation for proper ENV variables settings.
3. move.py is based on cython. The compiling script for this is not provided, please refer to cython documentation. However, you can remove cython part in the code, please refer to Running part for details.
4. No need to "install", as the code is for research purpose and not organize as "package".

## Running
1. You cannot use "spark-submit" to run the codes. Actually you should invoke a pyspark shell on the cluster
``` bash
pyspark --master spark://host:port
```
then copy the code from files following: region.py -> cal_user_trace.py -> cal_orig_area.py -> cal_mobility_matrix.py
2. cal_subarea_portion.py is for evaluation part in the paper.
3. cython is not necessary for move.py. As we tried, numba and cython have same performance improvement as transform all for loop into list comprehension (map/reduce in native python) in move.py. Thus we recommend to use list comprehension in move.py instead of more complicated cython and numba. 
