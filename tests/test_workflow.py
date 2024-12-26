import asyncio
from asyncflow.workflow import Message, Metadata, Workflow


def test_workflow():
    async def add(a, b):
        print(f'Adding {a} and {b}')
        await asyncio.sleep(1)
        return a + b

    async def multiply(a, b):
        print(f'Multiplying {a} and {b}')
        await asyncio.sleep(1)
        return a * b

    A = Metadata('A', '1.0')
    B = Metadata('B', '1.0')
    C = Metadata('C', '1.0')
    X = Metadata('X', '1.0')
    Y = Metadata('Y', '1.0')

    add_workflow = Workflow(add, [A, B], [X])
    multiply_workflow = Workflow(multiply, [X, C], [Y])

    a_to_add = asyncio.Queue()
    b_to_add = asyncio.Queue()
    x_to_multiply = asyncio.Queue()
    c_to_multiply = asyncio.Queue()
    y_output = asyncio.Queue()

    add_workflow.add_upstream(A, a_to_add)
    add_workflow.add_upstream(B, b_to_add)
    add_workflow.add_downstream(X, x_to_multiply)
    multiply_workflow.add_upstream(X, x_to_multiply)
    multiply_workflow.add_upstream(C, c_to_multiply)
    multiply_workflow.add_downstream(Y, y_output)

    async def run_workflow():
        await asyncio.gather(
            a_to_add.put(Message(A, 1)),
            b_to_add.put(Message(B, 2)),
            c_to_multiply.put(Message(C, 3))
        )
        await add_workflow()
        await multiply_workflow()
        result = await y_output.get()
        assert result.payload == 9

    asyncio.run(run_workflow())
