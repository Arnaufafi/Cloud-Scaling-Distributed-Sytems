import subprocess
import time
import random
import boto3
from concurrent.futures import ThreadPoolExecutor

# Configuración de SQS
QUEUE_NAME = 'text_queue'
RESULT_QUEUE = 'RESULTS'

sqs = boto3.client('sqs', region_name='us-east-1')

def get_queue_url(queue_name):
    response = sqs.get_queue_url(QueueName=queue_name)
    return response['QueueUrl']

def purge_queue(queue_url):
    try:
        sqs.purge_queue(QueueUrl=queue_url)
        print(f"[SQS] Cola purgada: {queue_url}")
    except Exception as e:
        print(f"[SQS] Error al purgar la cola: {e}")

def run_script(script):
    p = subprocess.Popen(["python3", script])
    p.wait()

def fill_queue():
    for i in range(10):  # Ejecutar N veces
        num_executions = random.randint(5, 40)
        client_scripts = (
            ["text_producer.py"] * num_executions +
            ["angry_producer.py"] * num_executions
        )

        print(f"[client] Iteración {i+1}: Llenando la cola con {num_executions} tareas...")
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(run_script, client_scripts)
        end_time = time.time()
        print(f"[client] Cola llena en {end_time - start_time:.2f} segundos.")

        time.sleep(1) 

if __name__ == '__main__':
    try:
        text_queue_url = get_queue_url(QUEUE_NAME)
        result_queue_url = get_queue_url(RESULT_QUEUE)
        purge_queue(text_queue_url)
        purge_queue(result_queue_url)

    except Exception as e:
        print("[ERROR] Asegúrate de que las colas existen en SQS. Detalles:", e)
    
    fill_queue()
