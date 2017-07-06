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
             
The first `map` is for parsing the JSON to a tuple of `(Int, UserEvent)`, where the Int is `UserEvent.id`. Then we pass the tuple to our `updateStateByKey` to do the rest.

## Drawbacks of `updateStateByKey`:

A major downside of using
<code class="highlighter-rouge">updateStateByKey</code>
is the fact that for each new incoming batch, the transformation iterates
<strong>the entire state store</strong>
, regardless of whether a new value for a given key has been consumed or not. This can effect performance especially when dealing with a large amount of state over time. There are
<a href="http://spark.apache.org/docs/latest/streaming-programming-guide.html#reducing-the-batch-processing-times">various ways to improving performance</a>
, but this still is a pain point.

No built in timeout mechanism - Think what would happen in our example, if the event signaling the end of the user session was lost, or hadn’t arrived for some reason. One upside to the fact
<code class="highlighter-rouge">updateStateByKey</code>
iterates all keys is that we can implement such a timeout ourselves, but this should definitely be a feature of the framework.
                      
What you receive is what you return - Since the return value from
<code class="highlighter-rouge">updateStateByKey</code>
is the same as the state we’re storing. In our case
<code class="highlighter-rouge">Option[UserSession]</code>
, we’re forced to return it downstream. But what happens if once the state is completed, I want to output a different type and use that in another transformation? Currently, that’s not possible.

## Advantages of `mapWithState` over `updateStateByKey`  :

code class="highlighter-rouge">mapWithState</code>
is
<code class="highlighter-rouge">updateStateByKey</code>
s successor released in Spark 1.6.0 as an
<em>experimental API</em>
. It’s the lessons learned down the road from working with stateful streams in Spark, and brings with it new and promising goods.

<code class="highlighter-rouge">mapWithState</code>
comes with features we’ve been missing from
<code class="highlighter-rouge">updateStateByKey</code>
:

1.<strong>Built in timeout mechanism</strong>
- We can tell
<code class="highlighter-rouge">mapWithState</code>
the period we’d like to hold our state for in case new data doesn’t come. Once that timeout is hit,
<code class="highlighter-rouge">mapWithState</code>
will be invoked one last time with a special flag (which we’ll see shortly).

2.<strong>Partial updates</strong>
- Only keys which have new data arrived in the current batch will be iterated. This means no longer needing to iterate the entire state store at every batch interval, which is a great performance optimization.

3.<strong>Choose your return type</strong>
- We can now choose a return type of our desire, regardless of what type our state object holds.

4.<strong>Initial state</strong>
- We can select a custom
<code class="highlighter-rouge">RDD</code>
to initialize our stateful transformation on startup.

Let’s take a look at the different parts that form the new API

The signature for
<code class="highlighter-rouge">mapWithState</code>
:

<code>
<span class="n">mapWithState</span>
<span class="o">[</span>
<span class="kt">StateType</span>
,
<span class="kt">MappedType</span>
<span class="o">](</span>
<span class="n">spec</span>
<span class="k">:</span>
<span class="kt">StateSpec</span>
<span class="o">[</span>
<span class="kt">K</span>
,
<span class="kt">V</span>
,
<span class="kt">StateType</span>
,
<span class="kt">MappedType</span>
<span class="o">])</span>
</code>




<p>As opposed to the <code class="highlighter-rouge">updateStateByKey</code> that required us to pass a function taking a sequence of messages and the state in the form of an
<code class="highlighter-rouge">Option[S]</code>, we’re now required to pass a <a href="http://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.streaming.StateSpec"><code class="highlighter-rouge">StateSpec</code></a>:</p>

<blockquote>
  <p>Abstract class representing all the specifications of the DStream transformation mapWithState operation of a pair DStream (Scala) or a JavaPairDStream (Java). Use the StateSpec.apply() or StateSpec.create() to create instances of this class.</p>
</blockquote>

<blockquote>
  <p>Example in Scala:</p>
</blockquote>

<div class="language-scala highlighter-rouge"><pre class="highlight"><code><span class="c1">// A mapping function that maintains an integer state and return a String
</span><span class="k">def</span> <span class="n">mappingFunction</span><span class="o">(</span><span class="n">key</span><span class="k">:</span> <span class="kt">String</span><span class="o">,</span> <span class="n">value</span><span class="k">:</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">Int</span><span class="o">],</span> <span class="n">state</span><span class="k">:</span> <span class="kt">State</span><span class="o">[</span><span class="kt">Int</span><span class="o">])</span><span class="k">:</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">String</span><span class="o">]</span> <span class="k">=</span> <span class="o">{</span>
  <span class="c1">// Use state.exists(), state.get(), state.update() and state.remove()
</span>  <span class="c1">// to manage state, and return the necessary string
</span><span class="o">}</span>

<span class="k">val</span> <span class="n">spec</span> <span class="k">=</span> <span class="nc">StateSpec</span><span class="o">.</span><span class="n">function</span><span class="o">(</span><span class="n">mappingFunction</span><span class="o">)</span>
</code></pre>
</div>

<p>The interesting bit is <code class="highlighter-rouge">StateSpec.function</code>, a factory method for creating the <code class="highlighter-rouge">StateSpec</code>.
it requires a function which has the following signature:</p>

<div class="language-scala highlighter-rouge"><pre class="highlight"><code><span class="n">mappingFunction</span><span class="k">:</span> <span class="o">(</span><span class="kt">KeyType</span><span class="o">,</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">ValueType</span><span class="o">],</span> <span class="nc">State</span><span class="o">[</span><span class="kt">StateType</span><span class="o">])</span> <span class="k">=&gt;</span> <span class="nc">MappedType</span>
</code></pre>
</div>

<p><code class="highlighter-rouge">mappingFunction</code> takes several arguments. Let’s construct them to match our example:</p>

<ol>
  <li><code class="highlighter-rouge">KeyType</code> - Obviously the key type, <code class="highlighter-rouge">Int</code></li>
  <li><code class="highlighter-rouge">Option[ValueType]</code> - Incoming data type, <code class="highlighter-rouge">Option[UserEvent]</code></li>
  <li><code class="highlighter-rouge">State[StateType]</code> - State to keep between iterations, <code class="highlighter-rouge">State[UserSession]</code></li>
  <li><code class="highlighter-rouge">MappedType</code> - Our return type, which can be anything. For our example we’ll pass an <code class="highlighter-rouge">Option[UserSession]</code>.</li>
</ol>

<h4 id="differences-between-mapwithstate-and-updatestatebykey">Differences between <code class="highlighter-rouge">mapWithState</code> and <code class="highlighter-rouge">updateStateByKey</code></h4>

<ol>
  <li>The value of our key, which previously wasn’t exposed.</li>
  <li>The incoming new values in the form of <code class="highlighter-rouge">Option[S]</code>, where previously it was a <code class="highlighter-rouge">Seq[S]</code>.</li>
  <li>Our state is now encapsulated in an object of type <code class="highlighter-rouge">State[StateType]</code></li>
  <li>We can return any type we’d like from the transformation, no longer bound to the type of the state we’re holding.</li>
</ol>

<p>(<em>There exists a more advanced API where we also receive a <code class="highlighter-rouge">Time</code> object, but we won’t go into that here. Feel free to check out
the different overloads <a href="http://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.streaming.StateSpec$">here</a></em>).</p>

<h3 id="exploring-state-management-with-the-state-object">Exploring state management with the <code class="highlighter-rouge">State</code> object</h3>

<p>Previously, managing our state meant working with an <code class="highlighter-rouge">Option[S]</code>. In order to update our state, we would
create a new instance and return that from our transformation. When we wanted to remove the state, we’d return
<code class="highlighter-rouge">None</code>. Since we’re now free to return any type from <code class="highlighter-rouge">mapWithState</code>, we need a way to interact with Spark to express
what we wish to do with the state in every iteration. For that, we have the <code class="highlighter-rouge">State[S]</code> object.</p>

<p>There are several methods exposed by the object:</p>

<ol>
  <li><code class="highlighter-rouge">def exists(): Boolean</code> - Checks whether a state exists</li>
  <li><code class="highlighter-rouge">def get(): S</code> - Get the state if it exists, otherwise it will throw <code class="highlighter-rouge">java.util.NoSuchElementException.</code> (We need to be careful with this one!)</li>
  <li><code class="highlighter-rouge">def isTimingOut(): Boolean</code> - Whether the state is timing out and going to be removed by the system after the current batch.</li>
  <li><code class="highlighter-rouge">def remove(): Unit</code> - Remove the state if it exists.</li>
  <li><code class="highlighter-rouge">def update(newState: S): Unit</code> - Update the state with a new value</li>
  <li><code class="highlighter-rouge">def getOption(): Option[S]</code> - Get the state as an <code class="highlighter-rouge">scala.Option</code>.</li>
</ol>

<p>Which we will soon see.</p>

<h3 id="changing-our-code-to-conform-to-the-new-api">Modifying our code as per new API</h3>

<p>Let’s rebuild our previous <code class="highlighter-rouge">updateUserEvents</code> to conform to the new API. Our new method signature now looks like this:</p>

<div class="language-scala highlighter-rouge"><pre class="highlight"><code><span class="k">def</span> <span class="n">updateUserEvents</span><span class="o">(</span><span class="n">key</span><span class="k">:</span> <span class="kt">Int</span><span class="o">,</span> <span class="n">value</span><span class="k">:</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">UserEvent</span><span class="o">],</span> <span class="n">state</span><span class="k">:</span> <span class="kt">State</span><span class="o">[</span><span class="kt">UserSession</span><span class="o">])</span><span class="k">:</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">UserSession</span><span class="o">]</span>
</code></pre>
</div>

<p>Instead of receiving a <code class="highlighter-rouge">Seq[UserEvent]</code>, we now receive each event individually.</p>

<p>Let’s go ahead and make those changes:</p>

<div class="language-scala highlighter-rouge"><pre class="highlight"><code><span class="k">def</span> <span class="n">updateUserEvents</span><span class="o">(</span><span class="n">key</span><span class="k">:</span> <span class="kt">Int</span><span class="o">,</span>
                     <span class="n">value</span><span class="k">:</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">UserEvent</span><span class="o">],</span>
                     <span class="n">state</span><span class="k">:</span> <span class="kt">State</span><span class="o">[</span><span class="kt">UserSession</span><span class="o">])</span><span class="k">:</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">UserSession</span><span class="o">]</span> <span class="k">=</span> <span class="o">{</span>
  <span class="cm">/*
  Get existing user events, or if this is our first iteration
  create an empty sequence of events.
  */</span>
  <span class="k">val</span> <span class="n">existingEvents</span><span class="k">:</span> <span class="kt">Seq</span><span class="o">[</span><span class="kt">UserEvent</span><span class="o">]</span> <span class="k">=</span>
    <span class="n">state</span>
      <span class="o">.</span><span class="n">getOption</span><span class="o">()</span>
      <span class="o">.</span><span class="n">map</span><span class="o">(</span><span class="k">_</span><span class="o">.</span><span class="n">userEvents</span><span class="o">)</span>
      <span class="o">.</span><span class="n">getOrElse</span><span class="o">(</span><span class="nc">Seq</span><span class="o">[</span><span class="kt">UserEvent</span><span class="o">]())</span>

  <span class="cm">/*
  Extract the new incoming value, appending the new event with the old
  sequence of events.
  */</span>
  <span class="k">val</span> <span class="n">updatedUserSession</span><span class="k">:</span> <span class="kt">UserSession</span> <span class="o">=</span>
    <span class="n">value</span>
      <span class="o">.</span><span class="n">map</span><span class="o">(</span><span class="n">newEvent</span> <span class="k">=&gt;</span> <span class="nc">UserSession</span><span class="o">(</span><span class="n">newEvent</span> <span class="o">+:</span> <span class="n">existingEvents</span><span class="o">))</span>
      <span class="o">.</span><span class="n">getOrElse</span><span class="o">(</span><span class="nc">UserSession</span><span class="o">(</span><span class="n">existingEvents</span><span class="o">))</span>

<span class="cm">/*
Look for the end event. If found, return the final `UserSession`,
If not, update the internal state and return `None`
*/</span>      
  <span class="n">updatedUserSession</span><span class="o">.</span><span class="n">userEvents</span><span class="o">.</span><span class="n">find</span><span class="o">(</span><span class="k">_</span><span class="o">.</span><span class="n">isLast</span><span class="o">)</span> <span class="k">match</span> <span class="o">{</span>
    <span class="k">case</span> <span class="nc">Some</span><span class="o">(</span><span class="k">_</span><span class="o">)</span> <span class="k">=&gt;</span>
      <span class="n">state</span><span class="o">.</span><span class="n">remove</span><span class="o">()</span>
      <span class="nc">Some</span><span class="o">(</span><span class="n">updatedUserSession</span><span class="o">)</span>
    <span class="k">case</span> <span class="nc">None</span> <span class="k">=&gt;</span>
      <span class="n">state</span><span class="o">.</span><span class="n">update</span><span class="o">(</span><span class="n">updatedUserSession</span><span class="o">)</span>
      <span class="nc">None</span>
  <span class="o">}</span>
<span class="o">}</span>
</code></pre>
</div>

<p>For each iteration of <code class="highlighter-rouge">mapWithState</code>:</p>

<ol>
  <li>In case this is our first iteration the state will be empty. We need to create it and append the new event.
if it isn’t, we already have existing events, extract them from the <code class="highlighter-rouge">State[UserSession]</code> and append the new event with the old events.</li>
  <li>Look for the <code class="highlighter-rouge">isLast</code> event flag. If it exists, remove the <code class="highlighter-rouge">UserSession</code> state and return an <code class="highlighter-rouge">Option[UserSession]</code>. Otherwise, update the state and return <code class="highlighter-rouge">None</code></li>
</ol>

<p>The choice to return <code class="highlighter-rouge">Option[UserSession]</code> the transformation is up to us. We could of chosen to return <code class="highlighter-rouge">Unit</code> and send
the complete <code class="highlighter-rouge">UserSession</code> from <code class="highlighter-rouge">mapWithState</code> as we did with <code class="highlighter-rouge">updateStateByKey</code>. But, I like it better that we can pass <code class="highlighter-rouge">UserSession</code> down the line to Another
tranformation to do more work as needed.</p>

<p>Our new Spark DAG now looks like this:</p>

<div class="language-scala highlighter-rouge"><pre class="highlight"><code><span class="k">val</span> <span class="n">stateSpec</span> <span class="k">=</span> <span class="nc">StateSpec</span><span class="o">.</span><span class="n">function</span><span class="o">(</span><span class="n">updateUserEvents</span> <span class="k">_</span><span class="o">)</span>

<span class="n">kafkaStream</span>
  <span class="o">.</span><span class="n">map</span><span class="o">(</span><span class="n">deserializeUserEvent</span><span class="o">)</span>
  <span class="o">.</span><span class="n">mapWithState</span><span class="o">(</span><span class="n">stateSpec</span><span class="o">)</span>
</code></pre>
</div>

<p>But, there’s one more thing to add. Since we don’t save the <code class="highlighter-rouge">UserSession</code> inside the transformation, we need
to add an additional transformation to store it in the persistent storage. For that, we can use <code class="highlighter-rouge">foreachRDD</code>:</p>

<div class="language-scala highlighter-rouge"><pre class="highlight"><code><span class="n">kafkaStream</span>
  <span class="o">.</span><span class="n">map</span><span class="o">(</span><span class="n">deserializeUserEvent</span><span class="o">)</span>
  <span class="o">.</span><span class="n">mapWithState</span><span class="o">(</span><span class="n">stateSpec</span><span class="o">)</span>
  <span class="o">.</span><span class="n">foreachRDD</span> <span class="o">{</span> <span class="n">rdd</span> <span class="k">=&gt;</span>
    <span class="k">if</span> <span class="o">(!</span><span class="n">rdd</span><span class="o">.</span><span class="n">isEmpty</span><span class="o">())</span> <span class="o">{</span>
      <span class="n">rdd</span><span class="o">.</span><span class="n">foreach</span><span class="o">(</span><span class="n">maybeUserSession</span> <span class="k">=&gt;</span> <span class="n">maybeUserSession</span><span class="o">.</span><span class="n">foreach</span><span class="o">(</span><span class="n">saveUserSession</span><span class="o">))</span>
    <span class="o">}</span>
  <span class="o">}</span>
</code></pre>
</div>

<p>(<em>If the connection to the underlying persistent storage is an expensive one which you don’t want to open foreach value
in the RDD, consider using <code class="highlighter-rouge">rdd.foreachPartition</code> instead of <code class="highlighter-rouge">rdd.foreach</code> (but that is beyond the scope of this post</em>)</p>

<h3 id="finishing-off-with-timeout">Finishing off with timeout</h3>

<p>In reality, when working with large amounts of data we have to shield ourselves from data lose. With our current
implementation if the <code class="highlighter-rouge">isLast</code> even doesn’t show, we’ll end up with that users actions “stuck” in the state.</p>

<p>Adding a timeout is simple:</p>

<ol>
  <li>Add the timeout when constructing our <code class="highlighter-rouge">StateSpec</code>.</li>
  <li>Handle the timeout in the stateful transformation.</li>
</ol>

<p>The first step is easily achieved by:</p>

<div class="language-scala highlighter-rouge"><pre class="highlight"><code><span class="k">import</span> <span class="nn">org.apache.spark.streaming._</span>
<span class="k">val</span> <span class="n">stateSpec</span> <span class="k">=</span>
  <span class="nc">StateSpec</span>
    <span class="o">.</span><span class="n">function</span><span class="o">(</span><span class="n">updateUserEvents</span> <span class="k">_</span><span class="o">)</span>
    <span class="o">.</span><span class="n">timeout</span><span class="o">(</span><span class="nc">Minutes</span><span class="o">(</span><span class="mi">5</span><span class="o">))</span>
</code></pre>
</div>

<p>(<code class="highlighter-rouge">Minutes</code> is a Sparks wrapper class for Scala’s <code class="highlighter-rouge">Duration</code> class.)</p>

<p>For our <code class="highlighter-rouge">updateUserEvents</code>, we need to monitor the <code class="highlighter-rouge">State[S].isTimingOut</code> flag to know we’re timing out.
Two things I want to mention in regards to timing out:</p>

<ol>
  <li>It’s important to note that once a timeout occurs, our <code class="highlighter-rouge">value</code> argument will be <code class="highlighter-rouge">None</code> (explaining why we recieve an <code class="highlighter-rouge">Option[S]</code> instead of <code class="highlighter-rouge">S</code> for value. More on that <a href="http://stackoverflow.com/questions/38397688/spark-mapwithstate-api-explanation/38397937#38397937">here</a>).</li>
  <li>If <code class="highlighter-rouge">mapWithState</code> is invoked due to a timeout, we <strong>must not call <code class="highlighter-rouge">state.remove()</code></strong>, that will be done on our behalf by the framework.
From the documentation of <code class="highlighter-rouge">State.remove</code>:</li>
</ol>

<blockquote>
  <p>State cannot be updated if it has been already removed (that is, remove() has already been called) or it is going to be removed due to timeout (that is, isTimingOut() is true).</p>
</blockquote>

<p>Let’s modify the code:</p>

<div class="language-scala highlighter-rouge"><pre class="highlight"><code><span class="k">def</span> <span class="n">updateUserEvents</span><span class="o">(</span><span class="n">key</span><span class="k">:</span> <span class="kt">Int</span><span class="o">,</span>
                     <span class="n">value</span><span class="k">:</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">UserEvent</span><span class="o">],</span>
                     <span class="n">state</span><span class="k">:</span> <span class="kt">State</span><span class="o">[</span><span class="kt">UserSession</span><span class="o">])</span><span class="k">:</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">UserSession</span><span class="o">]</span> <span class="k">=</span> <span class="o">{</span>
  <span class="k">def</span> <span class="n">updateUserSession</span><span class="o">(</span><span class="n">newEvent</span><span class="k">:</span> <span class="kt">UserEvent</span><span class="o">)</span><span class="k">:</span> <span class="kt">Option</span><span class="o">[</span><span class="kt">UserSession</span><span class="o">]</span> <span class="k">=</span> <span class="o">{</span>
    <span class="k">val</span> <span class="n">existingEvents</span><span class="k">:</span> <span class="kt">Seq</span><span class="o">[</span><span class="kt">UserEvent</span><span class="o">]</span> <span class="k">=</span>
      <span class="n">state</span>
        <span class="o">.</span><span class="n">getOption</span><span class="o">()</span>
        <span class="o">.</span><span class="n">map</span><span class="o">(</span><span class="k">_</span><span class="o">.</span><span class="n">userEvents</span><span class="o">)</span>
        <span class="o">.</span><span class="n">getOrElse</span><span class="o">(</span><span class="nc">Seq</span><span class="o">[</span><span class="kt">UserEvent</span><span class="o">]())</span>

    <span class="k">val</span> <span class="n">updatedUserSession</span> <span class="k">=</span> <span class="nc">UserSession</span><span class="o">(</span><span class="n">newEvent</span> <span class="o">+:</span> <span class="n">existingEvents</span><span class="o">)</span>

    <span class="n">updatedUserSession</span><span class="o">.</span><span class="n">userEvents</span><span class="o">.</span><span class="n">find</span><span class="o">(</span><span class="k">_</span><span class="o">.</span><span class="n">isLast</span><span class="o">)</span> <span class="k">match</span> <span class="o">{</span>
      <span class="k">case</span> <span class="nc">Some</span><span class="o">(</span><span class="k">_</span><span class="o">)</span> <span class="k">=&gt;</span>
        <span class="n">state</span><span class="o">.</span><span class="n">remove</span><span class="o">()</span>
        <span class="nc">Some</span><span class="o">(</span><span class="n">updatedUserSession</span><span class="o">)</span>
      <span class="k">case</span> <span class="nc">None</span> <span class="k">=&gt;</span>
        <span class="n">state</span><span class="o">.</span><span class="n">update</span><span class="o">(</span><span class="n">updatedUserSession</span><span class="o">)</span>
        <span class="nc">None</span>
    <span class="o">}</span>
  <span class="o">}</span>

  <span class="n">value</span> <span class="k">match</span> <span class="o">{</span>
    <span class="k">case</span> <span class="nc">Some</span><span class="o">(</span><span class="n">newEvent</span><span class="o">)</span> <span class="k">=&gt;</span> <span class="n">updateUserSession</span><span class="o">(</span><span class="n">newEvent</span><span class="o">)</span>
    <span class="k">case</span> <span class="k">_</span> <span class="k">if</span> <span class="n">state</span><span class="o">.</span><span class="n">isTimingOut</span><span class="o">()</span> <span class="k">=&gt;</span> <span class="n">state</span><span class="o">.</span><span class="n">getOption</span><span class="o">()</span>
  <span class="o">}</span>
<span class="o">}</span>
</code></pre>
</div>

<p>I’ve extracted the updating of the user actions to a local method, <code class="highlighter-rouge">updateUserSession</code>, which we call if we’re invoked as a result
of a new incoming value or else, we’re timing out the need to return user events we’ve accumulated so far.</p>

