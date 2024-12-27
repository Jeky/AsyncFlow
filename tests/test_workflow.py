import asyncio

import pytest
from asyncflow.workflow import Message, Metadata, Step, Workflow, WorkflowConfigError


A = Metadata('A', '1.0')
B = Metadata('B', '1.0')
C = Metadata('C', '1.0')
X = Metadata('X', '1.0')
Y = Metadata('Y', '1.0')

def test_steps_producing_same_metadata():
    async def f1(a):
        return a
    
    async def f2(a):
        return a
    
    workflow = Workflow()
    workflow.add(f1, [A], X)
    workflow.add(f2, [A], X)
    
    with pytest.raises(WorkflowConfigError):
        workflow.initialize()
    

def test_workflow():
    async def add(a, b):
        print(f'Adding {a} and {b}')
        return a + b

    async def multiply(a, b):
        print(f'Multiplying {a} and {b}')
        return a * b

    workflow = Workflow()
    workflow.add(add, [A, B], X)
    workflow.add(multiply, [X, C], Y)
    
    workflow.initialize()
    
    async def run_workflow():
        workflow.feed(A, 2)
        workflow.feed(B, 3)
        workflow.feed(C, 4)
        
        await workflow.run()
        
        assert await workflow.read(Y) == 20
    
    asyncio.run(run_workflow())
