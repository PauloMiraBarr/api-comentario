import boto3
import json
import os
from datetime import datetime

# Configuración de cliente S3
s3 = boto3.client('s3')
bucket_name = os.environ["S3_BUCKET_NAME"]

def lambda_handler(event, context):
    for record in event['Records']:
        # Verifica si el evento es una inserción de nuevo registro
        if record['eventName'] == 'INSERT':
            # Extrae los datos del evento de DynamoDB
            new_record = record['dynamodb']['NewImage']
            tenant_id = new_record['tenant_id']['S']
            uuid = new_record['uuid']['S']
            detalle = new_record['detalle']['M']
            texto = detalle['texto']['S']
            
            # Crear el contenido a almacenar en S3
            comentario = {
                'tenant_id': tenant_id,
                'uuid': uuid,
                'texto': texto,
                'timestamp': str(datetime.utcnow())
            }
            
            # Generar un nombre de archivo único basado en uuid
            file_name = f"comentarios/{tenant_id}/{uuid}.json"
            
            # Almacenar el archivo en S3
            s3.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=json.dumps(comentario),
                ContentType='application/json'
            )
            
            print(f"Comentario {uuid} almacenado en S3 como {file_name}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Stream procesado correctamente')
    }
