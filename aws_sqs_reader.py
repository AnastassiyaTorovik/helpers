import asyncio
from aiobotocore import session
import os

CONSUMERS_COUNT = 10
SQS_URL = os.getenv('SQS_URL')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')


async def get_messages_from_queue(queue_url: str, client: str) -> list:
    messages = []
    while True:
        resp = await client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10
        )

        try:
            messages.extend(resp['Messages'])
        except KeyError:
            break

        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]

        resp = await client.delete_message_batch(
            QueueUrl=queue_url, Entries=entries
        )

        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )

    return [message['Body'] for message in messages]


def create_multiple_sqs_consumers(client: str) -> list:
    tasks = []
    for i in range(CONSUMERS_COUNT):
        tasks.append(asyncio.create_task(get_messages_from_queue(SQS_URL, client)))
    return tasks


async def run_multiple_sqs_consumers() -> list:
    """reads data from AWS SQS using asynchronous consumers to reduce waiting time as much as possible"""
    aiosession = session.AioSession()
    async with aiosession.create_client("sqs", region_name='eu-west-1', aws_access_key_id=ACCESS_KEY,
                                        aws_secret_access_key=SECRET_KEY) as client:
        tasks = create_multiple_sqs_consumers(client)
        polled_messages = await asyncio.gather(*tasks)
    return [message for messages_list in polled_messages if messages_list for message in messages_list]

if __name__ == "__main__":
    asyncio.run(run_multiple_sqs_consumers())