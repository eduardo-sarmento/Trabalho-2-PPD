Trabalho 1.3 - Processamento Paralelo e Distribuído

Professor: Rodolfo da Silva Villaca
Grupo : Eduardo Sarmento e Pedro Paternostro

O trabalho utiliza o broker mosquito como middleware para a implementação e fiscalização das filas, para fazer o download e a instalação do broker é apenas necessario acessar o seguinte link e seguir as instruções para o seu sistema operacional: https://mosquitto.org/download/


O formato da mensagem para inserir um numero na DHT é numero inteiro entre 0 e 2^32-1 + "," +  numero inteiro entre 0 e 2^32-1, os numeros em formato de string

O formato da mensagem para fazer o get de um numero na DHT é numero inteiro entre 0 e 2^32-1 em formato de int

Para a execução do programa, basta executar o arquivo Trabalho_2_dht.py e Trabalho_2_client.py em quantos terminais desejar, não há limite de instâncias para nenhum dos dois. Os nós da DHT executam durante 80 segundos, depois fecham automaticamente.

A implementação do trabalho encontra-se excepcionalmente comentada, em adição a isso, ao executar o trabalho cada programa imprime dicas do estado em que se encontra e de mudanças que ocorrem, para que o usuário entenda com clareza tudo o que está sendo feito.

Em um funcionamento normal o programa deve imprimir:

- Avisos de eventos de JOIN e LEAVE da DHT
- O estado da DHT depois de uma alteração no número de nós
- Avisos de geração de número aleatório por parte do cliente
- Avisos de recebimento de mensagens de PUT e GET, com informações sobre a operação efetuada
- Avisos de recebimento de confirmações de PUT e GET (GET OK e PUT OK) por parte do cliente
- Qual é a hereditariedade, isto é, qual nó herda os elementos de qual, durante uma alteração no número de nós
- Avisos de recebimento de mensagens de FORCEPUT, usada para população da DHT de novos nós em caso de JOIN