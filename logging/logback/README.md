Frack Logging
=============

This library is meant for use with the Frack API. It adds an additional formatting parameter to the standard PatternLayout from Logback in order to identify the pipeline ID from which the log entry came. This will allow better log aggregation across the Frack framework.

The following is an example logback properties file utilizing the ezbake.frack.logging.FrackPatternLayoutEncoder class:

```
<configuration>

  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="ezbake.frack.logback.FrackPatternLayoutEncoder">
        <pattern>%-5level - %msg%n</pattern>
    </encoder>
  </appender>

  <root level="DEBUG">
    <appender-ref ref="STDOUT" />
  </root>
</configuration>
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

With the above example pattern of ` %-5level - %msg%n`, we would see the following in the log (assuming the worker has been added to the "test" pipeline):

```
[test] INFO  - hi!
```

No additional work is needed from the pipeline implementer in order to set the pipeline ID for the logger, this is all taken care of behind the scenes in the internals of Frack.