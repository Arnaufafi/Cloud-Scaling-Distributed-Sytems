import lithops
import boto3

# Censor list
CENSOR_LIST = ['fool', 'insult', 'idiot', 'stupid', 'dumb']

# Bucket y ruta
BUCKET_NAME = 'my-insults-bucket'
INPUT_PREFIX = 'texts/'

#Queue
QUEUE_NAME = 'text_and_store'

# S3 client
s3 = boto3.client('s3')

sqs = boto3.client('sqs', region_name='us-east-1')

def get_queue_url(queue_name):
    response = sqs.get_queue_url(QueueName=queue_name)
    return response['QueueUrl']

def list_s3_files(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [f's3://{bucket}/{obj["Key"]}' for obj in response.get('Contents', []) if obj["Key"].endswith('.txt')]

def send_censor_text(text):
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=text
        )
        print(f"[TextProducer] Sent: {text} (MessageId: {response['MessageId']})")
    except Exception as e:
        print("Error sending message:", e)
    

def map_function(obj):
  
    CENSOR_LIST = ['fool', 'insult', 'idiot', 'stupid', 'dumb']
    
    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = sqs.get_queue_url(QueueName='text_and_store')['QueueUrl']
    
    raw_data = obj.data_stream.read().decode('utf-8')
    counter = {}
    words = raw_data.lower().split()

    for word in words:
        word = word.strip(".,!?;:\"'")
        if word in CENSOR_LIST:
            counter[word] = counter.get(word, 0) + 1

    # Enviar texto a la cola
    try:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=raw_data
        )
    except Exception as e:
        print("Error sending message:", e)

    return counter

def reduce_function(results):
    total = {}
    for partial in results:
        for insult, count in partial.items():
            total[insult] = total.get(insult, 0) + count
    return total

if __name__ == '__main__':
    # Obtener lista de archivos en el bucket
    iterdata = list_s3_files(BUCKET_NAME, INPUT_PREFIX)
    
    queue_url = get_queue_url(QUEUE_NAME)

    fexec = lithops.FunctionExecutor(log_level='INFO')
    fexec.map_reduce(map_function, iterdata, reduce_function)
    result = fexec.get_result()

    print("Total de insultos censurados por palabra:")
    for insult, count in result.items():
        print(f"{insult}: {count}")
