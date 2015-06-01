### Overview:
Frack is a framework that makes streaming ingest development easy, and requires limited knowledge of distributed computing. Developers can easily write very effective and complex processing Pipelines with very little code, and testing/deployment of Pipelines is even easier. Frack does not rely on one streaming ingest backend, and can be used with new technologies as they appear.

### Architecture:
Frack is composed of an API with several key classes that should be implemented/extended by developers in order to create a processing pipeline. A Pipeline class encapsulates the flow of execution for a Frack streaming ingest process. A Pipeline is instantiated and Pipes are registered and connected in order to form a graph of processing that can be executed.
All processing classes in Frack are derived from a base class called Pipe, which has common functions that are shared by all processing components. These classes are the following:
Worker: This class receives data from other Pipes and processes that data. The processed data can be broadcast onto a message queue, output to another Worker, or sent to a dataset. Incoming and outgoing data takes the form of a thrift struct.
Generator: This class generates new streams of data. Generators are flexible and allow for the collection of data from any source that the developer sees fit. The data from a Generator is output to Workers in the Pipeline for processing/broadcasting.
Listener: This is a convenience class that is used in place of a Generator when a data source takes the form of a message queue. Listeners are instantiated in a Pipeline and given topics to listen to. All data picked up off of the message queue is forwarded to connected Workers.
Pipelines are created in code and then built into an archive for submission to the Frack backend. This is done with a thrift service that receives the archive as a binary object.

### Security:
The built in messaging library, EZBroadcast, uses the EzSecurity thrift service in conjunction with SSL certificates and keys provided by the Pipeline implementer to retrieve the authorizations granted to a Pipeline. This means that each piece of data coming into a Pipeline via a message queue is filtered based on the classification of the data against the authorizations granted to the Pipeline.
All data moving through a Pipeline is tied to an Accumulo visibility string which describes the visibility of the data. This allows Pipeline implementers to view the visibility of incoming data, as well as have the appropriate visibility information for any information leaving the Pipeline (sending the data to the data warehouse, a dataset, broadcasting, etc).

### Testing:
The components of Frack are tested in a variety of ways. Many of the different Frack libraries are unit tested individually. Unit/Integration testing is very difficult in some cases due to the dependence of the Frack libraries on other pieces of the EzBake framework.
Users of Frack can easily test Pipelines locally (on a VM or mac/linux machine) using the EventBus ingest system and RedisMQ message queue. EventBus simulates a distributed ingest system using only a single local java process. RedisMQ is a message queue backed by Redis, which can be easily downloaded and started up with no dependencies.
