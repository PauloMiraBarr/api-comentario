import boto3
import json
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = os.environ['S3_BUCKET_NAME']  # Nombre del bucket por stage
    
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            comentario_data = record['dynamodb']['NewImage']
            tenant_id = comentario_data['tenant_id']['S']
            texto = comentario_data['detalle']['M']['texto']['S']
            uuid = comentario_data['uuid']['S']
            
            comentario_json = {
                'tenant_id': tenant_id,
                'uuid': uuid,
                'detalle': {'texto': texto}
            }
            
            file_name = f"comentario-{uuid}.json"
            
            # Subir el comentario como archivo JSON a S3
            s3.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=json.dumps(comentario_json),
                ContentType='application/json'
            )
    
    return {
        'statusCode': 200,
        'message': 'Comentario guardado en S3 correctamente.'
    }
