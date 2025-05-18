def lambda_handler(event, context):
    # Por defecto, si no llega censor_list, usa esta lista
    default_censor_list = ['fool', 'insult', 'idiot', 'stupid', 'dumb']
    
    censor_list = event.get('censor_list', default_censor_list)
    
    message = event['Records'][0]['body']
    words = message.split()
    censored = ['****' if word.lower() in [w.lower() for w in censor_list] else word for word in words]
    result = ' '.join(censored)
    
    print("Censored:", result)
    
    return {
        'statusCode': 200,
        'body': result
    }
