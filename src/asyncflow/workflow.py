import asyncio
from asyncio import Queue
from typing import Callable, Dict, List, Any


class Metadata:
    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version

    def __str__(self):
        return f"{self.name}-{self.version}"

    def __repr__(self):
        return str(self)


class Message:
    def __init__(self, metadata: Metadata, payload: Any):
        self.metadata = metadata
        self.payload = payload

    def __str__(self):
        return f"Message({self.metadata}, {self.payload})"

    def __repr__(self):
        return str(self)


class WorkflowConfigError(Exception):
    def __init__(self, message):
        super().__init__(message)


class Step:
    def __init__(self, task: Callable, consumes: List[Metadata], produce: Metadata = None):
        self.task = task
        self.consumes = consumes
        self.produce = produce
        self.upstreams: Dict[Metadata, Queue] = {}
        self.downstreams: Dict[Metadata, List[Queue]] = {}

    def add_upstream(self, metadata: Metadata, upstream: Queue) -> 'Step':
        self.upstreams[metadata] = upstream
        return self

    def add_downstream(self, metadata: Metadata, downstream: Queue) -> 'Step':
        self.downstreams.setdefault(metadata, []).append(downstream)
        return self

    async def __call__(self) -> Any:
        input_messages = await self._wait_for_upstreams()
        inputs = self._convert_messages_to_inputs(input_messages)
        result = await self.task(*inputs)
        await self._push_output_to_downstreams(result)
        return result

    async def _wait_for_upstreams(self) -> List[Message]:
        messages = await asyncio.gather(*[upstream.get() for upstream in self.upstreams.values()])
        print(f"Step {self} received messages: {messages}")
        for queue in self.upstreams.values():
            queue.task_done()
        return messages

    def _convert_messages_to_inputs(self, messages: List[Message]) -> List[Any]:
        metadata_to_input = {
            message.metadata: message.payload for message in messages}
        return [metadata_to_input[metadata] for metadata in self.consumes]

    async def _push_output_to_downstreams(self, result: Any):
        print(f"Step {self} produced {result}")
        if self.produce:
            message = Message(self.produce, result)
            print(
                f"Step {self} pushing {message} to downstreams {self.downstreams}")
            for metadata, downstream_queues in self.downstreams.items():
                for queue in downstream_queues:
                    print(f'Pushing {message} to {hex(id(queue))}')
                    queue.put_nowait(message)

    def __str__(self):
        return f"Step({self.task.__name__})"

    def __repr__(self):
        return str(self)


class Workflow:
    def __init__(self):
        self.steps: List[Step] = []
        self.upstreams: Dict[Metadata, List[Queue]] = {}
        self.downstreams: Dict[Metadata, Queue] = {}

    def add_step(self, step: Step) -> 'Workflow':
        self.steps.append(step)
        return self

    def add(self, task: Callable, consumes: List[Metadata], produces: List[Metadata] = None) -> 'Workflow':
        step = Step(task, consumes, produces)
        return self.add_step(step)

    def initialize(self):
        for step in self.steps:
            for metadata in step.consumes:
                queue = Queue()
                self.upstreams.setdefault(metadata, []).append(queue)
                step.add_upstream(metadata, queue)
                print(f"connecting {hex(id(queue))} to {step}")

        for step in self.steps:
            if step.produce:
                produce_metadata = step.produce
                if produce_metadata in self.downstreams:
                    raise WorkflowConfigError(
                        f"Metadata {produce_metadata} produced by multiple steps")

                queue = Queue()
                self.downstreams[produce_metadata] = queue
                step.add_downstream(produce_metadata, queue)
                print(
                    f"connecting {hex(id(self.downstreams[produce_metadata]))} back to workflow")

                if produce_metadata in self.upstreams:
                    for queue in self.upstreams[produce_metadata]:
                        print(f"connecting {step} to {hex(id(queue))}")
                        step.add_downstream(produce_metadata, queue)

    async def run(self):
        await asyncio.gather(*[step() for step in self.steps])

    def feed(self, metadata: Metadata, payload: Any):
        if metadata not in self.upstreams:
            raise WorkflowConfigError(
                f"Metadata {metadata} not consumed by any steps")

        message = Message(metadata, payload)
        for queue in self.upstreams[metadata]:
            queue.put_nowait(message)

    async def read(self, metadata: Metadata):
        if metadata not in self.downstreams:
            raise WorkflowConfigError(
                f"Metadata {metadata} not produced by any steps")

        message = await self.downstreams[metadata].get()
        return message.payload
