import paho.mqtt.client as mqtt
from random import randrange, randint
import time

DHT = {}
nodes = []
nodes_leave_ack = []
leaving = False
ID = randint(0, (2**8)-1)
    
mqttBroker ="127.0.0.1"

def on_message(client, userdata, message):
    print("received message: " ,str(message.payload.decode("utf-8")))

# Mensagem de JOIN
# O novo no eh adicionado por todos os outros, que enviam uma confirmacao de recebimento
# O antigo no responsavel entrega ao novo no a parcela da DHT que compete a ele
def on_message_join(client, userdata, message):
    print("joined: " ,str(message.payload.decode("utf-8")))
    join_ID = int(message.payload.decode("utf-8"))
    nodes.append(join_ID)
    nodes.sort()
    payload = str(ID)
    client.publish("rsv/join_response", payload)
    index = nodes.index(ID)
    if(join_ID == nodes[index-1]):
        transfer_on_join(nodes[(index-2)%len(nodes)],join_ID)


# Resposta ao JOIN
# Com essa mensagem o novo no adiciona os outros e mantem sua lista correta
def on_message_join_response(client, userdata, message):
    response_ID = int(message.payload.decode("utf-8"))
    if(not nodes.count(response_ID) == 1):
        nodes.append(response_ID)
        nodes.sort()
    print("List of DHT nodes: ", nodes)

# O no que esta saindo envia essa mensagem aos outros
# Os nos que recebem removem o ID do remetente da sua lista e enviam uma confirmacao
def on_message_leave(client, userdata, message):
    leave_ID = int(message.payload.decode("utf-8"))
    print(leave_ID, " is leaving!")
    nodes.remove(leave_ID)
    payload = str(ID)
    client.publish("rsv/leave_response", payload)
    print("List of DHT nodes: ", nodes)

# O no que esta saindo lista todos que respondem a sua mensagem de saida
# Quando todos os nos responderem, ele pode efetivamente sair!
def on_message_leave_response(client, userdata, message):
    if(leaving and int(message.payload.decode("utf-8")) != ID):
        nodes_leave_ack.append(int(message.payload.decode("utf-8")))

# Transferencia de responsabilidade 
# Popula o novo no com a parcela da DHT que compete a ele
def on_message_forceput(client, userdata, message):
    info = str(message.payload.decode("utf-8"))
    info = info.split(',')
    node_ID = int(info[0])
    if(node_ID == ID):
        key = int(info[1])
        randomNumber = int(info[2])
        DHT[key] = randomNumber
        index = nodes.index(ID)
        print("ForcePut successful: key=", key, "myID=", ID, "myIndex=", index)
        client.publish("rsv/put_ok", ID)

# Mensagem de PUT
# O no checa se a operacao esta no intervalo que compete a ele
# Se estiver, efetua a operacao e publica uma confirmacao para o cliente
def on_message_put(client, userdata, message):
    info = str(message.payload.decode("utf-8"))
    info = info.split(',')
    key = int(info[0])
    randomNumber = int(info[1])
    index = nodes.index(ID)
    if(ID == nodes[0] and (key <= ID or key > nodes[-1])):
        #print("aqui ID == nodes[0]")
        DHT[key] = randomNumber
    elif(key <= ID and key > nodes[index-1]):
        #print("aqui key <= ID ")
        DHT[key] = randomNumber
    else:
        #print("aqui else")
        return
    print("Put successful: key=", key, "myID=", ID, "myIndex=", index)
    client.publish("rsv/put_ok", ID)

# Mensagem de GET
# O no checa se a operacao esta no intervalo que compete a ele
# Se estiver, efetua a operacao e publica uma confirmacao para o cliente
def on_message_get(client, userdata, message):
    key = int(message.payload.decode("utf-8"))

    index = nodes.index(ID)
    val = 0
    if(ID == nodes[0] and (key <= ID or key > nodes[-1])):
        val = DHT[key]
    elif(key <= ID and key > nodes[index-1]):
        val = DHT[key]
    else:
        return
    print("Get successful: value=", key)
    client.publish("rsv/get_ok", val)

# Quando um no sai, ele precisa transferir sua responsabilidade
# Transfere a DHT inteira do no para o novo responsavel (i.e. no seguinte)
def transfer_on_leave():
    for key in DHT:
        client.publish("rsv/put",  payload=str(key)+","+str(key))

# Quando um no entra, ele precisa ser encarregado de uma parcela da DHT
# Transfere uma parte da DHT do no para o novo responsavel (i.e. no anterior)
def transfer_on_join(previous_ID, join_ID):
    for key in list(DHT):
        if(key > previous_ID and key <= join_ID):
            client.publish("rsv/forceput",  payload=str(join_ID)+","+str(key)+","+str(key))
            del DHT[key]


### INICIALIZACAO DO NO DHT###
client = mqtt.Client("Node_" + str(ID))
client.connect(mqttBroker)
client.loop_start()

# Assina mensagens necessarias p/ funcionamento
client.subscribe("rsv/join")
client.subscribe("rsv/join_response")
client.subscribe("rsv/leave")
client.subscribe("rsv/leave_response")
client.subscribe("rsv/forceput")
client.subscribe("rsv/put")
client.subscribe("rsv/get")
client.message_callback_add('rsv/join', on_message_join)
client.message_callback_add('rsv/join_response', on_message_join_response)
client.message_callback_add('rsv/leave', on_message_leave)
client.message_callback_add('rsv/leave_response', on_message_leave_response)
client.message_callback_add('rsv/forceput', on_message_forceput)
client.message_callback_add('rsv/put', on_message_put)
client.message_callback_add('rsv/get', on_message_get)

# Publica JOIN!
client.publish("rsv/join", ID)
print("Just published " + str(ID) + " to topic rsv/join")

# O no precisa sair alguma hora, entao depois de 30 segundos ele da o sinal de saida
time.sleep(60)
leaving = True
print("I'm leaving!")
client.unsubscribe("rsv/put")
client.unsubscribe("rsv/get")
client.unsubscribe("rsv/join")
client.unsubscribe("rsv/join_response")
client.unsubscribe("rsv/forceput")
payload = str(ID)
client.publish("rsv/leave", payload)

# Espera todos os nos reconhecerem a sua saida
# Depois, transfere a parcela da DHT deste no para o sucessor (i.e. no seguinte)
while(nodes_leave_ack != nodes):
    time.sleep(1)
transfer_on_leave()
print(ID, " left!")
client.loop_stop()
