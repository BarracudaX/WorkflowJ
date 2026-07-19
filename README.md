# WorkflowJ

A workflow engine for executing workflows defined using code.

### Note that this project uses a preview feature of java 25 - Structured Concurrency.

## Goals
- Type-safe and fluent code.
- Help write fair workflows that avoiding monopolization of resources.
- Low RAM consumption by storing locally in the file system.
- Recovery with event-replaying.
- Caching of task results and other optimization.
- Encourage writing of business logic executed in workflow with pure functions.
- Distributed workflow execution(future)