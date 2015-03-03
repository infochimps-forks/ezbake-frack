Frack Logging
=============

This library is meant for use with the Frack API. It adds an additional formatting parameter to the standard PatternLayout from Log4j in order to identify the pipeline ID from which the log entry came. This will allow better log aggregation across the Frack framework.

The following is an example log4j properties file utilizing the ezbake.frack.log4j.FrackPatternLayout class:

```
# Root logger option
log4j.rootLogger=INFO, stdout

# Direct log messages to stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.Target=System.out
log4j.appender.stdout.layout=ezbake.frack.log4j.FrackPatternLayout
log4j.appender.stdout.layout.ConversionPattern=PIPELINE[%f] %-5p %c{1}:%L - %m%n
```

Here's some example code for setting up the logger and making an entry in the log:

```java
private static Logger log = LoggerFactory.getLogger(MyWorker.class);
...
@Override
public void process(T thriftObject) {
    log.info("hi!");
}
```

With the above example pattern of `PIPELINE[%f] %-5p %c{1} - %m%n`, we would see the following in the log (assuming the worker has been added to the "test" pipeline):

```
PIPELINE[test] INFO MyWorker - hi!
```

If a pattern is not set, the default FrackPatternLayout pattern is `[%f] %m%n` which simply outputs the pipeline ID in brackets followed by the message.

No additional work is needed from the pipeline implementer in order to set the pipeline ID for the logger, this is all taken care of behind the scenes in the internals of Frack.