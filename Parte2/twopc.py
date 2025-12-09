from enum import Enum
from pydistsim.algorithm.node_algorithm import NodeAlgorithm, StatusValues
from pydistsim.algorithm.node_wrapper import NodeAccess
from pydistsim.message import Message
from pydistsim.restrictions.communication import BidirectionalLinks
from pydistsim.restrictions.knowledge import InitialDistinctValues
from pydistsim.restrictions.reliability import TotalReliability
from pydistsim.restrictions.topological import Connectivity
from pydistsim.logging import logger
import random

class TwoPCAlgorithm(NodeAlgorithm):
    default_params = { "coordinatorID": 0 }

    class Status(StatusValues):
        COORDINATOR = "COORDINATOR"
        COORDINATOR_WAITING_PREPARED = "COORDINATOR_WAITING_PREPARED"
        COORDINATOR_WAITING_ACK = "COORDINATOR_WAITING_ACK"
        SLEEP = "SLEEP"
        WAITING = "WAITING"
        DONE = "DONE"

    S_init = (Status.COORDINATOR, Status.SLEEP,)
    S_term = (Status.DONE,)

    algorithm_restrictions = (
        BidirectionalLinks,
        InitialDistinctValues,
        Connectivity,
    )

    def initializer(self):
        self.apply_restrictions()

        coordinator = 1
        nodes = list(self.network.nodes())
        random.shuffle(nodes)

        if self.coordinatorID != 0:
            target_node = next((n for n in nodes if n.memory['unique_value'] == self.coordinatorID), None)
            if target_node:
                nodes.remove(target_node)
                nodes.insert(0, target_node)

        for node in nodes:
            neighbors_source = []
            for n in range(len(self.network.nodes()) - 1):
                neighbors_source.append({"source": n, "id": None})
            node.memory['neighbors'] = neighbors_source
            node.memory['count'] = len(nodes) - 1
            if coordinator == 1:
                coordinator = 0
                node.status = self.Status.COORDINATOR
                node.push_to_inbox(Message(meta_header=NodeAlgorithm.INI, destination=node))
                node.memory['node_status'] = {n: 'sleep' for n in range(len(self.network.nodes()) - 1)}
            else:
                node.status = self.Status.SLEEP

    @Status.COORDINATOR
    def spontaneously(self, node, message):
        # Coordinator inicia el proceso 2PC enviando PREPARE a todos los nodos

        data = { "id": node.memory['unique_value'] }

        self.send_msg(node, Message(header="Prepare", data=data, destination=list(node.neighbors())))
        self.set_alarm(node, 20, Message(header="Timeout_Prepared", destination=node))
        node.status = self.Status.COORDINATOR_WAITING_PREPARED
    

    @Status.COORDINATOR_WAITING_PREPARED
    def alarm(self, node: NodeAccess, message):
        # Manejo de timeout esperando PREPARED
        # Si no recibo PREPARED de algun nodo, le reenvio PREPARE

        if message.header == "Timeout_Prepared":
            node_status = node.memory['node_status']
            failed = False
            for neighbor_id, status in node_status.items():
                if status == 'sleep':
                    failed = True
                    logger.info(f"Coordinator Node {node.memory['unique_value']} did not receive PREPARED from Node source {neighbor_id}. Resending Prepared to him.")
                    self.send_msg(node, Message(header="Prepare", data={"id": node.memory['unique_value']}, destination=[n for n in node.neighbors() if n.id == neighbor_id]))

            if failed:
                self.set_alarm(node, 20, Message(header="Timeout_Prepared", destination=node))
        else:
            logger.info(f"Coordinator Node {node.memory['unique_value']} in COORDINATOR_WAITING_PREPARE state received unexpected message with header {message.header}.")
    
    @Status.COORDINATOR_WAITING_PREPARED
    def receiving(self, node, message):
        # Recibo los mensajes PREPARED de los nodos
        # Cuando recibo todos, envio COMMIT o ABORT segun corresponda

        if message.header == "Prepared":
            neighbors_source = node.memory['neighbors']
            for neighbor in neighbors_source:
                if neighbor['source'] == message.source.id:
                    neighbor['id'] = message.data['id']
                    break

            node.memory['count'] -= 1

            if message.data['decision'] == 0:
                node.memory['decision'] = "Abort"
                self.send_msg(node, Message(header="Abort", data={"id": node.memory['unique_value']}, destination=list(node.neighbors())))
                logger.info(f"Coordinator Node {node.memory['unique_value']} received PREPARED with ABORT from Node {message.data['id']}.")
                for neighbor_id in node.memory['node_status'].keys():
                    self.set_alarm(node, 20, Message(header=f"Timeout_Ack_{neighbor_id}", destination=node))
                node.memory['count'] = len(node.neighbors())
                node.status = self.Status.COORDINATOR_WAITING_ACK
                return
            
            received_id = message.source.id
            logger.info(f"Coordinator Node {node.memory['unique_value']} received PREPARED from Node {received_id}.")
            node.memory['node_status'][received_id] = 'prepared'
            
            if node.memory['count'] == 0:
                node.memory['decision'] = "Commit"
                logger.info(f"Coordinator Node {node.memory['unique_value']} received PREPARED from all neighbors. Sending COMMIT.")
                self.send_msg(node, Message(header="Commit", data={"id": node.memory['unique_value']}, destination=list(node.neighbors())))
                for neighbor_id in node.memory['node_status'].keys():
                    self.set_alarm(node, 20, Message(header=f"Timeout_Ack_{neighbor_id}", destination=node))
                node.memory['count'] = len(node.neighbors())
                node.status = self.Status.COORDINATOR_WAITING_ACK
        else:
            logger.info(f"Coordinator Node {node.memory['unique_value']} in COORDINATOR_WAITING_PREPARE state received unexpected alarm message with header {message.header}. Ignoring")

    @Status.COORDINATOR_WAITING_ACK
    def receiving(self, node, message):
        # Recibo los mensajes ACK de los nodos
        # Cuando recibo todos, termino el proceso

        if message.header == "Ack":
            node.memory['count'] -= 1
            node.memory['node_status'][message.source.id] = 'ack'

            if node.memory['count'] == 0:
                logger.info(f"Coordinator Node {node.memory['unique_value']} received ACK from all neighbors. Done.")
                node.status = self.Status.DONE
        else:
            logger.info(f"Coordinator Node {node.memory['unique_value']} in COORDINATOR_WAITING_ACK state received unexpected message with header {message.header}.")
    
    @Status.COORDINATOR_WAITING_ACK
    def alarm(self, node, message):
        # Manejo de timeout esperando ACK
        # Si no recibo ACK de algun nodo, le reenvio COMMIT

        if message.header.startswith("Timeout_Ack_"):
            failed_id = int(message.header.split("_")[-1])
            if node.memory['node_status'][failed_id] != 'ack':
                logger.info(f"Coordinator Node {node.memory['unique_value']} timed out waiting for ACK message from source #{failed_id}. Resending COMMIT.")
                self.send_msg(node, Message(header=node.memory['decision'], data={"id": node.memory['unique_value']}, destination=[n for n in node.neighbors() if n.id == failed_id]))
                self.set_alarm(node, 20, Message(header=f"Timeout_Ack_{failed_id}", destination=node))
        else:
            logger.info(f"Coordinator Node {node.memory['unique_value']} in COORDINATOR_WAITING_ACK state received unexpected alarm message with header {message.header}. Ignoring")

    @Status.SLEEP
    def receiving(self, node, message):
        # Nodo recibe PREPARE del coordinador
        # Responde con PREPARED y pasa a estado WAITING

        neighbors_source = node.memory['neighbors']
        for neighbor in neighbors_source:
            if neighbor['source'] == message.source.id:
                neighbor['id'] = message.data['id']
                break
        node.memory['neighbors'] = neighbors_source

        if message.header == "Prepare":
            logger.info(f"Node {node.memory['unique_value']} received PREPARE from Node {message.data['id']}. Sending PREPARED.")
            self.send_msg(node, Message(header="Prepared", data={"id": node.memory['unique_value'], "decision": 1}, destination=message.source))
            node.status = self.Status.WAITING
        else:
            logger.info(f"Node {node.memory['unique_value']} in SLEEP state received unexpected message with header {message.header}.")

    @Status.WAITING
    def receiving(self, node, message):
        # Nodo en WAITING recibe COMMIT o ABORT del coordinador
        # Responde con ACK y pasa a estado DONE

        if message.header == "Commit":
            logger.info(f"Node {node.memory['unique_value']} received COMMIT from Node {message.data['id']}. Sending ACK.")
            self.send_msg(node, Message(header="Ack", data={"id": node.memory['unique_value']}, destination=message.source))
            node.status = self.Status.DONE
        elif message.header == "Abort":
            logger.info(f"Node {node.memory['unique_value']} received ABORT from Node {message.data['id']}. Aborting and going to DONE state.")
            self.send_msg(node, Message(header="Ack", data={"id": node.memory['unique_value']}, destination=message.source))
            node.status = self.Status.DONE
        elif message.header == "Prepare":
            logger.info(f"Node {node.memory['unique_value']} in WAITING state received duplicate PREPARE from Node {message.data['id']} probably his message didn’t arrive. Resending PREPARED.")
            self.send_msg(node, Message(header="Prepared", data={"id": node.memory['unique_value'], "decision": 1}, destination=message.source))
        else:
            logger.info(f"Node {node.memory['unique_value']} in WAITING state received unexpected message with header {message.header}.")

    @Status.DONE
    def default(self, node, message):
        # En estado DONE, solo respondo a mensajes duplicados reenviando mi respuesta

        if message.header.startswith("Timeout_"):
            logger.info(f"Node {node.memory['unique_value']} in DONE state received timeout message with header {message.header}, ignoring.")
        elif message.header in ["Commit", "Abort"]:
            logger.info(f"Node {node.memory['unique_value']} in DONE state received duplicate {message.header} message from coordinator, probably his message didn’t arrive. Resending.")
            self.send_msg(node, Message(header="Ack", data={"id": node.memory['unique_value']}, destination=message.source))
            pass
        else:
            raise Exception(f"Node {node.memory['unique_value']} is in DONE state and should not receive messages.")
    