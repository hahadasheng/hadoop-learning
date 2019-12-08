# 一、自定义复杂类型

access.log
    第二个字段：手机号
    倒数第三字段：上行流量
    倒数第二字段：下行流量

需求：统计每个手机号上行流量和、下行流量和、总的流量和(上行流量和+下行流量和)

Access.java
    手机号、上行流量、下行流量、总流量

既然要求和：根据手机号进行分组，然后把该手机号对应的上下行流量加起来

Mapper: 把手机号、上行流量、下行流量 拆开
    把手机号作为key，把Access作为value写出去

Reducer：(13726238888,<Access,Access>)

# Partition

```java
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;

/** 
 * Partitions the key space.
 * 
 * <p><code>Partitioner</code> controls the partitioning of the keys of the 
 * intermediate map-outputs. The key (or a subset of the key) is used to derive
 * the partition, typically by a hash function. The total number of partitions
 * is the same as the number of reduce tasks for the job. Hence this controls
 * which of the <code>m</code> reduce tasks the intermediate key (and hence the 
 * record) is sent for reduction.</p>
 * 
 * Note: If you require your Partitioner class to obtain the Job's configuration
 * object, implement the {@link Configurable} interface.
 * 
 * @see Reducer
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class Partitioner<KEY, VALUE> {
  
  /** 
   * Get the partition number for a given key (hence record) given the total 
   * number of partitions i.e. number of reduce-tasks for the job.
   *   
   * <p>Typically a hash function on a all or a subset of the key.</p>
   *
   * @param key the key to be partioned.
   * @param value the entry value.
   * @param numPartitions the total number of partitions.
   * @return the partition number for the <code>key</code>.
   */
  public abstract int getPartition(KEY key, VALUE value, int numPartitions);
  
}
```

默认实现类
```java
package org.apache.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Partitioner;

/** Partition keys by their {@link Object#hashCode()}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HashPartitioner<K, V> extends Partitioner<K, V> {

  /** Use {@link Object#hashCode()} to partition. */
  public int getPartition(K key, V value,
                          int numReduceTasks) {
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }
}

```


numReduceTasks:你的作业所指定的reducer的个数，决定了reduce作业输出文件的个数
HashPartitioner是MapReduce默认的分区规则,根据key的Hash值将key分配到对应的Reducer Task中去
reducer个数：3
1 % 3 = 1
2 % 3 = 2
3 % 3 = 0

需求：将统计结果按照手机号的前缀进行区分，并输出到不同的输出文件中去
    13* ==> ..
    15* ==> ..
    other ==> ..

<<<<<<< HEAD
Partitioner决定map task(或者Combiner)的输出的数据交由哪个reducetask处理
=======
Partitioner决定maptask输出的数据交由哪个reducetask处理
>>>>>>> a9d37a311ae07a749a2085cf4e67935b56c4854a
默认实现：分发的key的hash值与reduce task个数取模
