import paho.mqtt.client as mqtt 
from random import randrange, randint
import time

mqttBroker = "127.0.0.1" 
ID = randint(0, (2**8)-1)

def on_message(client, userdata, message):
    print("received message: " ,str(message.payload.decode("utf-8")))

# Resposta ao PUT
# Confirma que a operacao foi efetuada com sucesso
def on_message_put_ok(client, userdata, message):
    dht_node_id = int(message.payload.decode("utf-8"))
    print("put OK! Node ID: ", dht_node_id)

# Resposta ao GET
# Confirma que a operacao foi efetuada com sucesso
def on_message_get_ok(client, userdata, message):
    stored_value = int(message.payload.decode("utf-8"))
    print("get OK! Stored value: ", stored_value)

# INICIALIZACAO DO CLIENTE #
client = mqtt.Client("Node_Client_" + str(ID))
client.connect(mqttBroker)
client.loop_start()

# Assina mensagens necessarias p/ funcionamento
client.subscribe("rsv/put_ok")
client.subscribe("rsv/get_ok")
client.message_callback_add('rsv/put_ok', on_message_put_ok)
client.message_callback_add('rsv/get_ok', on_message_get_ok)

# Enquanto estiver funcionando, gera numeros aleatorios e utiliza PUT e GET
# Obs: Para manter a simplicidade, todos os elementos da DHT tem o mesmo valor que a chave
while True:
    randNumber = randint(0, (2**8)-1)
    print(randNumber)
    client.publish("rsv/put",  payload=str(randNumber)+","+str(randNumber))
    client.publish("rsv/get", randNumber)
    time.sleep(10)