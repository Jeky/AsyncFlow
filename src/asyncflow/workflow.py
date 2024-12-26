import asyncio
from asyncio import Queue
from typing import Callable, List, Any


class Metadata:
    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version


class Message:
    def __init__(self, metadata: Metadata, payload: Any):
        self.metadata = metadata
        self.payload = payload


class Workflow:
    def __init__(self, task: Callable, consumes: List[Metadata], produces: List[Metadata] = None):
        self.task = task
        self.consumes = consumes
        self.produces = produces or []
        self.upstreams = {}
        self.downstreams = {}

    def add_upstream(self, metadata: Metadata, upstream: Queue):
        self.upstreams[metadata] = upstream

    def add_downstream(self, metadata: Metadata, downstream: Queue):
        self.downstreams[metadata] = downstream

    async def __call__(self):
        input_messages = await self._wait_for_upstreams()
        inputs = self._convert_messages_to_inputs(input_messages)
        result = await self.task(*inputs)
        await self._push_output_to_downstreams(result)
        return result

    async def _wait_for_upstreams(self):
        messages = await asyncio.gather(*[upstream.get() for upstream in self.upstreams.values()])
        for queue in self.upstreams.values():
            queue.task_done()
        return messages

    def _convert_messages_to_inputs(self, messages: List[Message]):
        metadata_to_input = {
            message.metadata: message.payload for message in messages}
        return [metadata_to_input[metadata] for metadata in self.consumes]

    async def _push_output_to_downstreams(self, result: Any):
        if self.produces:
            for metadata, downstream in self.downstreams.items():
                await downstream.put(Message(metadata, result))
