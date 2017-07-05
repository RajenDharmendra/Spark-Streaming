# Spark-Streaming 
Spark Streaming step by step with updateStateByKey and mapWithState

# Introduction

Apache Spark is composed of several modules, each serving different purposes. One of it’s powerfull modules is the Streaming API, which gives the developer the power of working with a continuous stream (or micro batches to be accurate) under an abstraction called Discretized Stream, or DStream.

In this section we will look into a particular property of Spark Streaming, it’s stateful streaming API. Stateful Streaming enables us to maintain state between micro batches, allowing us to form sessionization of our data.

# Understanding with an Example
In order to understand how to work with the APIs, let’s create a simple example of incoming data which requires us to sessionize. Our input stream of data will be that of a `UserEvent` type:

                      case class UserEvent(id: Int, data: String, isLast: Boolean) 
 
Each event describes a unique user. We identify a user by his id, and a `String` representing the content of the event that occurred. We also want to know when the user has ended his session, so we’re provided with a `isLast` flag which indicates the end of the session.

Our state, responsible for aggregating all the user events, will be that of a `UserSession type`:

                      case class UserSession(userEvents: Seq[UserEvent])

Which contains the sequence of events that occurred for a particular user. For this example, we’ll assume our data source is a stream of JSON encoded data consumed from Kafka.

Our `Id` property will be used as the key, and the `UserEvent` will be our value. Together, we get a
`DStream[(Int, UserEvent)]`.

 # Important Key Points:
 
 ## Checkpointing is preliminary for stateful streaming:
 
Sparks mechanism of checkpointing is the frameworks way of guaranteeing fault tolerance through the lifetime of our spark job. When we’re operating 24/7, things will fail that might not be directly under our control, such as a network failure or datacenter crashes. To promise a clean way of recovery, Spark can checkpoint our data every interval of our choosing to a persistent data store, such as Amazon S3, HDFS or Azure Blob Storage, if we tell it to do so.

Checkpointing is a feature for any non-stateful transformation, but it is mandatory that you provide a checkpointing directory for stateful streams, otherwise your application won’t be able to start.

Providing a checkpoint directory is as easy as calling the `StreamingContext` with the directory location:

                      val sparkContext = new SparkContext()
                      val ssc = new StreamingContext(sparkContext, Duration(4000))
                      ssc.checkpoint("path/to/persistent/storage")

## Key value pairs in the DStream:

Stateful transformations require that we operate on a `DStream` which encapsulates a key value pair, in the form of 
`DStream[(K, V)]` where `K` is the type of the `key` and `V` is type the `value`. Working with such a stream allows Spark to shuffle data based on the key, so all data for a given key can be available on the same worker node and allow you to do meaningful aggregations.

The signature for `updateStateByKey` looks like this:

                      def updateStateByKey[S](updateFunc: (Seq[V], Option[S]) ⇒ Option[S])
                      
`updateStateByKey` requires a function which accepts: 
1.`Seq[V]` - The list of new values received for the given key in the current batch.
2.`Option[S]` - The state we’re updating on every iteration.

For the first invocation of our job, the state is going to be `None`, signaling it is the first batch for the given key. After that it’s entirely up to us to manage it’s value. Once we’re done with a particular state for a given key, we need to return `None` to indicate to Spark we don’t need the state anymore.

## Implementation:
              
                      def updateUserEvents(newEvents: Seq[UserEvent],
                    state: Option[UserSession]): Option[UserSession] = {
                    
Append the new events to the state. If this the first time we're invoked for the key we fallback to creating a new UserSession with the new events.
                         
                    val newState = state
                      .map(prev => UserSession(prev.userEvents ++ newEvents))
                      .orElse(Some(UserSession(newEvents)))
                     
If we received the `isLast` event in the current batch, save the session to the underlying store and return None to delete the state.Otherwise, return the accumulated state so we can keep updating it in the next batch.
                 
                    if (newEvents.exists(_.isLast)) {
                      saveUserSession(state)
                      None
                    } else newState
                  }

At each batch, we want to take the state for the given user and concat both old events and new events into a new `Option[UserSession]`. Then, we want to check if we’ve reached the end of this users session, so we check the newly arrived sequence for the `isLast` flag on any of the `UserEvents`. If we received the last message, we save the user action to some persistent storage, and then return `None` to indicate we’re done. If we haven’t received an end message, we simply return the newly created state for the next iteration.

                        val kafkaStream =
            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

                    kafkaStream
                      .map(deserializeUserEvent)
                      .updateStateByKey(updateUserEvents)
                      


                 





