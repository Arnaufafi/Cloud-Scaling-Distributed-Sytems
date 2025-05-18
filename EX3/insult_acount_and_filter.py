import lithops
import boto3
import uuid

# Lista de palabras a censurar
CENSOR_LIST = ['fool', 'insult', 'idiot', 'stupid', 'dumb']

# Configuración de S3
BUCKET_NAME = 'my-insults-bucket'
INPUT_PREFIX = 'texts/'
OUTPUT_PREFIX = 'filtered_texts/'

# Nombre de la cola SQS 
#QUEUE_NAME = 'text_and_store'

# Clientes
s3 = boto3.client('s3')
sqs = boto3.client('sqs', region_name='us-east-1')

def get_queue_url(queue_name):
    response = sqs.get_queue_url(QueueName=queue_name)
    return response['QueueUrl']

def list_s3_files(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [f's3://{bucket}/{obj["Key"]}' for obj in response.get('Contents', []) if obj["Key"].endswith('.txt')]

def map_function(obj):

    raw_data = obj.data_stream.read().decode('utf-8')
    words = raw_data.split()
    counter = {}
    censored_words = []

    for word in words:
        stripped = word.strip(".,!?;:\"'")
        lower = stripped.lower()
        if lower in CENSOR_LIST:
            counter[lower] = counter.get(lower, 0) + 1
            censored_words.append('****')
        else:
            censored_words.append(word)

    censored_text = ' '.join(censored_words)

    # Guardar resultado en S3
    file_key = OUTPUT_PREFIX + f"censored_{uuid.uuid4().hex}.txt"
    s3 = boto3.client('s3')  # Re-crear dentro del worker
    s3.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=censored_text)

    return counter

def reduce_function(results):
    total = {}
    for partial in results:
        for insult, count in partial.items():
            total[insult] = total.get(insult, 0) + count
    return total

if __name__ == '__main__':
    iterdata = list_s3_files(BUCKET_NAME, INPUT_PREFIX)

    fexec = lithops.FunctionExecutor(log_level='INFO')
    fexec.map_reduce(map_function, iterdata, reduce_function)
    result = fexec.get_result()

    print("Total de insultos censurados por palabra:")
    for insult, count in result.items():
        print(f"{insult}: {count}")