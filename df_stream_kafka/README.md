# Kafka Stream
This repository contains example of using KStream to read and write with Kafka data.

# Stream Windows
All the windowing operations output results at the end of the window. The output of the window will be single event based on the aggregate function used. The output event will have the time stamp of the end of the window and all window functions are defined with a fixed length.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-conceptual.png)
## Tumbling window
Tumbling window functions are used to segment a data stream into distinct time segments and perform a function against them, such as the example below. The key differentiators of a Tumbling window are that they repeat, do not overlap, and an event cannot belong to more than one tumbling window.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-tumbling-intro.png)
## Hopping window
Hopping window functions hop forward in time by a fixed period. It may be easy to think of them as Tumbling windows that can overlap, so events can belong to more than one Hopping window result set. To make a Hopping window the same as a Tumbling window, specify the hop size to be the same as the window size.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-hopping-intro.png)
## Sliding window
Sliding window functions, unlike Tumbling or Hopping windows, produce an output only when an event occurs. Every window will have at least one event and the window continuously moves forward by an € (epsilon). Like hopping windows, events can belong to more than one sliding window.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-sliding-intro.png)

## Session window 
Session window functions group events that arrive at similar times, filtering out periods of time where there is no data. It has three main parameters: timeout, maximum duration, and partitioning key (optional).

A session window begins when the first event occurs. If another event occurs within the specified timeout from the last ingested event, then the window extends to include the new event. Otherwise if no events occur within the timeout, then the window is closed at the timeout.

If events keep occurring within the specified timeout, the session window will keep extending until maximum duration is reached. The maximum duration checking intervals are set to be the same size as the specified max duration. For example, if the max duration is 10, then the checks on if the window exceed maximum duration will happen at t = 0, 10, 20, 30, etc.

When a partition key is provided, the events are grouped together by the key and session window is applied to each group independently. This partitioning is useful for cases where you need different session windows for different users or devices.
![](https://docs.microsoft.com/en-us/azure/stream-analytics/media/stream-analytics-window-functions/stream-analytics-window-functions-session-intro.png)

## Reference
1. [KStream SerDe Guide](https://docs.confluent.io/current/streams/developer-guide/datatypes.html#streams-developer-guide-serdes)
2. [Confluent Wire Format](https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format)
3. [KStream DSL Windows](https://docs.confluent.io/current/streams/developer-guide/dsl-api.html#windowing)
