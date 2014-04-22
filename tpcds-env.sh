# path to the tpcds-kit directory
export TPCDS_ROOT=$HOME/tpcds-kit

# top level directory for flat files in HDFS
export FLATFILE_HDFS_ROOT=/user/${USER}/tpcds

# scale factor in GB
# SF 3000 yields ~1TB for the store_sales table
export TPCDS_SCALE_FACTOR=1000

# this is used to determine the number of dsdgen processes to generate data
# usually set to one per physical CPU core
# example - 20 nodes @ 12 threads each
export DSDGEN_NODES=`grep -c ^ /root/spark-ec2/slaves`
# Assumes that the master is the same machine type as the slaves.
export DSDGEN_THREADS_PER_NODE=`grep -c ^processor /proc/cpuinfo`
export DSDGEN_TOTAL_THREADS=$((DSDGEN_NODES * DSDGEN_THREADS_PER_NODE))

# the name for the tpcds database
export TPCDS_DBNAME=tpcds
