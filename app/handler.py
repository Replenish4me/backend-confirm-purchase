import boto3
import json
import pymysql
import os
import datetime

def lambda_handler(event, context):
    
    # Leitura dos dados da requisição
    token = event['token']
    
    # Conexão com o banco de dados
    secretsmanager = boto3.client('secretsmanager')
    response = secretsmanager.get_secret_value(SecretId=f'replenish4me-db-password-{os.environ.get("env", "dev")}')
    db_password = response['SecretString']
    rds = boto3.client('rds')
    response = rds.describe_db_instances(DBInstanceIdentifier=f'replenish4medatabase{os.environ.get("env", "dev")}')
    endpoint = response['DBInstances'][0]['Endpoint']['Address']
    # Conexão com o banco de dados
    with pymysql.connect(
        host=endpoint,
        user='admin',
        password=db_password,
        database='replenish4me'
    ) as conn:
    
        # Verificação da sessão ativa no banco de dados
        with conn.cursor() as cursor:
            sql = "SELECT usuario_id FROM SessoesAtivas WHERE id = %s"
            cursor.execute(sql, (token,))
            result = cursor.fetchone()
            
            if result is None:
                response = {
                    "statusCode": 401,
                    "body": json.dumps({"message": "Sessão inválida"})
                }
                return response
            
            usuario_id = result[0]
            
            # Verificação do carrinho
            sql = "SELECT produto_id, quantidade FROM CarrinhoCompras WHERE usuario_id = %s"
            cursor.execute(sql, (usuario_id,))
            result = cursor.fetchall()
            
            if not result:
                response = {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Carrinho vazio"})
                }
                return response
            
            # Buscando endereço do usuário
            sql = "SELECT endereco FROM Usuarios WHERE id = %s"
            cursor.execute(sql, (usuario_id,))
            result = cursor.fetchone()

            if result is None:
                response = {
                    "statusCode": 404,
                    "body": json.dumps({"message": "Usuário não encontrado"})
                }
                return response

            endereco = result[0]

            if endereco is None:
                response = {
                    "statusCode": 400,
                    "body": json.dumps({"message": "Endereço não cadastrado"})
                }
                return response
            
            # Criação do pedido
            sql = "INSERT INTO Pedidos (usuario_id, data, status, endereco) VALUES (%s, %s, %s, %s)"
            formated_date = datetime.datetime.now().strftime('%Y-%m-%d')
            cursor.execute(sql, (usuario_id, formated_date, 'Pedido Realizado', endereco))
            pedido_id = cursor.lastrowid
            
            # Criação dos itens do pedido
            for produto_id, quantidade in result:
                sql = "INSERT INTO ItensPedido (pedido_id, produto_id, quantidade) VALUES (%s, %s, %s)"
                cursor.execute(sql, (pedido_id, produto_id, quantidade))
            
            # Limpeza do carrinho
            sql = "DELETE FROM CarrinhoCompras WHERE usuario_id = %s"
            cursor.execute(sql, (usuario_id,))
            
            conn.commit()


    # Retorno da resposta da função
    response = {
        "statusCode": 200,
        "body": json.dumps({"message": "Compra efetuada com sucesso"})
    }
    return response
