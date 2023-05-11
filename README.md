# Backend - Python

Repositório criado a partir do template Backend - Python

# Pastas

# Importante

## Não editar os código da pasta infra nem da pasta .github

## function

Pasta com o código que será executado no backend

### Arquivos:

- handler.py:

Arquivo contendo a função handler, que deverá ser alterada para que ela execute o objetivo da função

- \_\_init\_\_.py:

Arquivo utilizado para exportar a função que será definida no handler para que seja utilizada no azure functions

- \_\_main\_\_.py:

Arquivo utilzado para invocar a função handler localmente

Não sendo alterada esta estrutura, qualquer arquivo pode ser criado dentro da pasta function e utilizado no código

## tests

Pasta contendo os testes unitários, para criar um teste unitário deve ser criado um arquivo com o seguinte padrão de nome: test_*.py, contendo uma classe com funções de teste no mesmo formato do test_handler.py

### Arquivos:

- \_\_main\_\_.py

Responsável por invocar todos os arquivos de testes

- test_*.py

Arquivos que contém as classes de testes

# Executando os códigos

Para executar os testes unitários utilize o comando
```
python -m tests 
```

Para executar o código localmente utilize o comando
```
python -m function
```

