import boto3
import json
import time
import signal
import sys
from botocore.exceptions import ClientError

# Configuración
QUEUE_NAME = 'text_queue'
LAMBDA_FUNCTION_NAME = 'InsultFilterLambda'

# AWS
sqs = boto3.client('sqs', region_name='us-east-1')
lambda_client = boto3.client('lambda', region_name='us-east-1')

# URL SQS
queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)['QueueUrl']

running = True

def graceful_exit(signum=None, frame=None):
    global running
    print("Terminando consumo...")
    running = False

signal.signal(signal.SIGINT, graceful_exit)

print("Escuchando mensajes de SQS...")

while running:
    try:
        # Recibir mensajes de la cola
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=10
        )

        messages = response.get('Messages', [])
        if not messages:
            continue

        for msg in messages:
            receipt_handle = msg['ReceiptHandle']
            body = msg['Body']

            try:
                censor_list = ['fool', 'insult', 'idiot', 'stupid', 'dumb']
                lambda_response = lambda_client.invoke(
                    FunctionName=LAMBDA_FUNCTION_NAME,
                    InvocationType='RequestResponse',
                    Payload=json.dumps({
                        "Records": [{
                            "body": body
                        }],
                        "censor_list": censor_list
                    })
                )

                result_payload = lambda_response['Payload'].read().decode('utf-8')
                result = json.loads(result_payload).get('body')

                print(f"Mensaje procesado: {body}")
                print(f"Resultado: {result}")

                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

            except Exception as e:
                print("Error invocando Lambda:", e)

    except ClientError as e:
        if e.response['Error']['Code'] == 'ExpiredToken':
            print("Token expirado. Apagando el consumidor.")
            graceful_exit()
        else:
            print("Error al leer de SQS:", e)
    except Exception as e:
        print("Error desconocido al leer de SQS:", e)

print("Consumidor detenido.")
