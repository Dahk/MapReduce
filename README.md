# Function-based MapReduce
## How to use
In order to use this program, you must first set it up. First, you have to upload the WordCount and CountingWords map functions as well as the Reducer function to the IBM Functions service. Then, you have to fill the file `secret.yaml` with the corresponding parameters (urls, keys, etc). Finally, run the orchestrator.

```
python3 orchestrator.py CW|WC <bucket_name> <file_name> <npartitions>
```
 *The result file will state the mapper used and the name of the input file (i.e. WordCount-bible.txt)*
##### Examples:
&NewLine;
```
python3 orchestrator.py CW mybucket1 essay.txt 30
```
```
python3 orchestrator.py WC largeTextFiles bible.txt 5
```
---
## Explanation
MapReduce is a programming model and implementation to enable the parallel processing of huge amounts of data. In a nutshell, it breaks a large dataset into smaller chunks to be processed separately on different worker nodes and automatically gathers the results across the multiple nodes to return a single result. 

In this case, our model consists of 3 parts: *Splitting*, *Mapping* and *Reducing*. Each of these parts take place on different parts of the program. 

The Splitting part occurs in the orchestrator. The orchestrator is the *main* program, which uploads the desired text file to the cloud, dispatches *n* map functions on the cloud and eventually calls the *reducer* that fetches all the partial results and merges them into the final result. The Mapping part happens when the *mappers* get called. These mappers process a chunk of data each one specified by the orchestrator (that chunk of data is calculated based on the number of partitions that the user specifies as a parameter). After a mapper has completed its process, it uploads its result to the cloud and notifies the reducer via the queue system. While these mappers are working, the orchestrator has already called the reducer on the cloud. This one is waiting synchronously for messages on the queue. These messages contain the name of the result file on its body, so that the reducer downloads it and accumulates it in a temporary variable that will get uploaded again to the cloud when it has finally received all the mappers' results.

## Our MapReduce
The objective of this practical assignment was to write the whole MapReduce system with two implementations of the map function. These map functions are the following ones:

**CountingWords**: Counts the total number of words in a text file. For example, given the following text: "I love Distributed Systems", the output of CountingWords should be 4 words.
**WordCount**: Counts the number of occurrences of each word in a text file. For instance, given the following text: "foo bar bar foo", the output of WordCount should be: bar, 2; foo, 2.

The program had to be able to scale depending on the number of partitions or threads specified by the user.

We decided to implement the orchestrator as a Pyhton class, so that it cloud be used for different approaches in the future. However, we also provided a *main* program for testing.

---

### Techonologies Used
- IBM Cloud Object Storage
- IBM Functions
- RabbitAMQP
- Python

### Authors
Pol Roca Llaberia (<pol.roca@estudiants.urv.cat>)
Marc Bachs Ciprés (<marc.bachs@estudiants.urv.cat>)