import boto3
import json
import threading
import time

#AWS
sqs = boto3.client('sqs', region_name='us-east-1')
queue_name = 'text_queue'
queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']

lambda_client = boto3.client('lambda', region_name='us-east-1')
lambda_function_name = 'InsultFilterLambda'

max_workers = 10
worker_semaphore = threading.Semaphore(max_workers)

running = True

def worker():
    while running:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )
            messages = response.get('Messages', [])
            if not messages:
                # Sleep to avoid busy waiting
                time.sleep(1)
                continue

            msg = messages[0]
            receipt_handle = msg['ReceiptHandle']
            body = msg['Body']

            # Invoca Lambda
            payload = {
                "Records": [{"body": body}],
                "censor_list": ['fool', 'insult', 'idiot', 'stupid', 'dumb']
            }

            lambda_response = lambda_client.invoke(
                FunctionName=lambda_function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )

            result_payload = lambda_response['Payload'].read().decode('utf-8')
            result = json.loads(result_payload).get('body')
            print(f"Mensaje procesado: {body}")
            print(f"Resultado: {result}")

            # Delate message from the queue if filtered
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        except Exception as e:
            print(f"Error en worker: {e}")

def stream(function, maxfunc, queue_url):
    global running
    workers = []
    while running:
        attributes = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        msg_count = int(attributes['Attributes']['ApproximateNumberOfMessages'])
        print(f"Mensajes en cola: {msg_count}")

        # Decide how many workers we should launch
        to_launch = min(msg_count, maxfunc) - len(workers)
        if to_launch > 0:
            print(f"Lanzando {to_launch} workers")
            for _ in range(to_launch):
                # Control semaphore in order to respect the max limit
                worker_semaphore.acquire()
                t = threading.Thread(target=worker_wrapper)
                t.daemon = True
                t.start()
                workers.append(t)

       #clean workers if they have finished
        workers = [w for w in workers if w.is_alive()]

        # Sleep in order to not charge the server
        time.sleep(1)

def worker_wrapper():
    try:
        worker()
    finally:
        worker_semaphore.release()

if __name__ == "__main__":
    try:
        stream(lambda_client.invoke, max_workers, queue_url)
    except KeyboardInterrupt:
        print("Deteniendo stream...")
        running = False
