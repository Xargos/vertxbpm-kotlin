## Simple Process Engine
The goal of this project is to build a fast, asynchronous, non-blocking, high availability, distributed process engine.

This is achieved by implementing the engine using a clustered vertx backed by Apache Ignite for data grid functionality and ACID transactions.

Right now this is a very simple engine whose workflows can only be executed using one path and have their state always stored in Ignite cache.

####To Do:
- Add basic HttpVerticle performance tests.
- Add choice capability to the workflows and engine.
- Test performance using enabled Ignite persistance.
- Add workflow steps that wait for user input.
- Add self-healing when local verticle fails.
- Add static analysis tools.
- Add process execution without saving steps.
- Fix logging
- Make it more configurable
- Add process visualisation (dashboard)
