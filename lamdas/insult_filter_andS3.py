import boto3
import uuid

s3 = boto3.client('s3')
BUCKET_NAME = 'my-insults-bucket'
OUTPUT_PREFIX = 'filtered_texts/'  # Carpeta donde guardar los archivos

def lambda_handler(event, context):
    default_censor_list = ['fool', 'insult', 'idiot', 'stupid', 'dumb']
    censor_list = event.get('censor_list', default_censor_list)

    message = event['Records'][0]['body']
    words = message.split()
    censored = ['****' if word.lower() in [w.lower() for w in censor_list] else word for word in words]
    result = ' '.join(censored)

    file_key = OUTPUT_PREFIX + f"censored_{uuid.uuid4().hex}.txt"
    s3.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=result)

    return {
        'statusCode': 200,
        'body': f'Censored text stored at s3://{BUCKET_NAME}/{file_key}'
    }
