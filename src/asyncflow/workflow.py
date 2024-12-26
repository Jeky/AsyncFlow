from asyncio import Queue
import asyncio
from typing import Callable, List


class Metadata(object):

    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version


class Message(object):
    def __init__(self, metadata: Metadata, payload):
        self.metadata = metadata
        self.payload = payload


class Workflow(object):

    def __init__(self, task: Callable,
                 consumes: List[Metadata], produces: List[Metadata]|None = None):
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
    
    def _wait_for_upstreams(self):
        messages = asyncio.gather(*[upstream.get() for upstream in self.upstreams.values()])
        for metadata, queue in self.upstreams.items():
            queue.task_done()
        return messages
    
    def _convert_messages_to_inputs(self, messages):
        metadata_to_input = {
            message.metadata: message.payload for message in messages}
        return [metadata_to_input[metadata] for metadata in self.consumes]
    
    async def _push_output_to_downstreams(self, result):
        if self.produces:
            for metadata, downstream in self.downstreams.items():
                await downstream.put(Message(metadata, result))


if __name__ == '__main__':
    async def add(a, b):
        print(f'Adding {a} and {b}')
        await asyncio.sleep(1)
        return a + b
    
    async def multiply(a, b):
        print(f'Multiplying {a} and {b}')
        await asyncio.sleep(1)
        return a * b
    
    # now we define a workflow steps:
    # input a,b,c
    # add a,b -> x
    # multiply x,c -> y
    # output y
    
    A = Metadata('A', '1.0')
    B = Metadata('B', '1.0')
    C = Metadata('C', '1.0')
    X = Metadata('X', '1.0')
    Y = Metadata('Y', '1.0')
    
    add_workflow = Workflow(add, [A, B], [X])
    multiply_workflow = Workflow(multiply, [X, C], [Y])
    
    # now we need to connect the workflows
    a_to_add = Queue()
    b_to_add = Queue()
    x_to_multiply = Queue()
    c_to_multiply = Queue()
    y_output = Queue()
    
    add_workflow.add_upstream(A, a_to_add)
    add_workflow.add_upstream(B, b_to_add)
    add_workflow.add_downstream(X, x_to_multiply)
    multiply_workflow.add_upstream(X, x_to_multiply)
    multiply_workflow.add_upstream(C, c_to_multiply)
    multiply_workflow.add_downstream(Y, y_output)
    
    # now we can run the workflows
    async def run_workflow():
        await asyncio.gather(a_to_add.put(Message(A, 1)),
                             b_to_add.put(Message(B, 2)),
                             c_to_multiply.put(Message(C, 3)))
        
        await add_workflow()
        await multiply_workflow()
        
        result = await y_output.get()
        print(f'Final result: {result.payload}')
        
    asyncio.run(run_workflow())
    
    
