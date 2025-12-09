from enum import Enum
from pydistsim.algorithm.node_algorithm import NodeAlgorithm, StatusValues
from pydistsim.algorithm.node_wrapper import NodeAccess
from pydistsim.message import Message
from pydistsim.restrictions.communication import BidirectionalLinks
from pydistsim.restrictions.knowledge import InitialDistinctValues
from pydistsim.restrictions.topological import Connectivity
from pydistsim.logging import logger
import random
import hashlib

class ThreePCByzantineAlgorithm(NodeAlgorithm):
    required_params = ["m"]

    class Status(StatusValues):
        COORDINATOR = "COORDINATOR"
        COORDINATOR_WAITING_PREPARED = "COORDINATOR_WAITING_PREPARED"
        COORDINATOR_WAITING_ACK = "COORDINATOR_WAITING_ACK"
        COORDINATOR_WAITING_DONE = "COORDINATOR_WAITING_DONE"
        COORDINATOR_ABORTING = "COORDINATOR_ABORTING"
        SLEEP = "SLEEP"
        WAITING_PRECOMMIT = "WAITING_PRECOMMIT"
        WAITING_COMMIT = "WAITING_COMMIT"
        FAULTY = "FAULTY"
        DONE = "DONE"

    S_init = (Status.COORDINATOR, Status.SLEEP, Status.FAULTY)
    S_term = (Status.DONE,)

    algorithm_restrictions = (
        BidirectionalLinks,
        InitialDistinctValues,
        Connectivity,
    )

    def initializer(self):
        self.apply_restrictions()

        nodes = list(self.network.nodes())
        n = len(nodes)
        random.shuffle(nodes)
        
        # Si no paso coordinador, elijo el nodo random como coordinador
        if self.coordinatorID != 0:
            target_node = next((n for n in nodes if n.memory['unique_value'] == self.coordinatorID), None)
            if target_node:
                nodes.remove(target_node)
                nodes.insert(0, target_node)

        coordinator = True
        faulty_count = 0
        for _, node in enumerate(nodes):
            node.memory['m'] = self.m
            node.memory['n'] = n
            node.memory['prepare_votes'] = {}  # {node_id: (decision, signature)}
            node.memory['ack_votes'] = {}  # {node_id: signature}
            node.memory['node_status'] = {}
            node.memory['private_key'] = f"key_{node.memory['unique_value']}"  # Dummy key
            
            if not coordinator and faulty_count < self.m:
                node.status = self.Status.FAULTY
                faulty_count += 1
                logger.info(f"Node {node.memory['unique_value']} initialized as FAULTY")
            elif coordinator:
                coordinator = False
                node.status = self.Status.COORDINATOR
                node.push_to_inbox(Message(meta_header=NodeAlgorithm.INI, destination=node))
                logger.info(f"Node {node.memory['unique_value']} initialized as COORDINATOR (Byzantine tolerant, m={self.m}, quorum={self.n - 1})")
            else:
                node.status = self.Status.SLEEP
                logger.info(f"Node {node.memory['unique_value']} initialized as REPLICA")

    @Status.COORDINATOR
    def spontaneously(self, node: NodeAccess, message):
        # El coordinador inicia el protocolo enviando PREPARE a todos los replicas
        # Siempre firma su mensaje y comprueba firmas de los demas

        node.memory['node_status'] = {other.id: 'sleep' for other in node.neighbors() if other != node}

        data_str = f"prepare:{node.memory['unique_value']}"
        signature = self.sign_message(node, data_str)
        
        data = {
            "id": node.memory['unique_value'],
            "signature": signature
        }

        logger.info(f"COORDINATOR {node.memory['unique_value']} sends SIGNED PREPARE (sig={signature}) to all replicas")
        self.send_msg(node, Message(header="Prepare", data=data, destination=list(node.neighbors())))
        self.set_alarm(node, 20, Message(header="Timeout_Prepared", destination=node))
        node.status = self.Status.COORDINATOR_WAITING_PREPARED
    
    @Status.COORDINATOR_WAITING_PREPARED
    def alarm(self, node: NodeAccess, message):
        # Si no recibo PREPARED de algun nodo, lo reenvio

        if message.header == "Timeout_Prepared":
            node_status = node.memory['node_status']
            failed = False
            for neighbor_id, status in node_status.items():
                if status == 'sleep':
                    failed = True
                    logger.info(f"Coordinator {node.memory['unique_value']} timeout: no PREPARED from node {neighbor_id}. Resending.")
                    data_str = f"prepare:{node.memory['unique_value']}"
                    signature = self.sign_message(node, data_str)
                    data = {"id": node.memory['unique_value'], "signature": signature}
                    self.send_msg(node, Message(header="Prepare", data=data, 
                                               destination=[n for n in node.neighbors() if n.id == neighbor_id]))
            if failed:
                self.set_alarm(node, 20, Message(header="Timeout_Prepared", destination=node))
    
    @Status.COORDINATOR_WAITING_PREPARED
    def receiving(self, node: NodeAccess, message):
        # Recibo PREPARED de replicas, verifico firmas y cuento votos
        # Si recibo todos los votos, y ninguno es ABORT, envio PRECOMMIT

        if message.header == "Prepared":
            sender_id = message.source.id
            decision = message.data['decision']
            signature = message.data['signature']
            
            # Verifico firma
            data_str = f"prepared:{message.data['id']}:{decision}"
            if not self.verify_signature(node, data_str, signature, message.data['id']):
                logger.info(f"Coordinator {node.memory['unique_value']} rejects PREPARED from {message.data['id']}: invalid signature")
                return
            
            # Guardo voto
            node.memory['prepare_votes'][message.data['id']] = (decision, signature)
            node.memory['node_status'][sender_id] = 'prepared'
            
            logger.info(f"Coordinator {node.memory['unique_value']} received VALID signed PREPARED from {message.data['id']} (decision={decision})")
            
            # Chequeo si hay que abortar
            if decision == 0:
                logger.info(f"Coordinator {node.memory['unique_value']} received ABORT vote. Aborting transaction.")
                self.send_abort(node)
                return
            
            if len(node.memory['prepare_votes']) >= self.n - 1:
                commit_votes = sum(1 for d, _ in node.memory['prepare_votes'].values() if d == 1)
                
                if commit_votes >= self.n - 1:
                    logger.info(f"Coordinator {node.memory['unique_value']} achieved QUORUM ({commit_votes}/{self.n - 1}). Sending PRECOMMIT.")
                    
                    # Sign and send PreCommit
                    data_str = f"precommit:{node.memory['unique_value']}"
                    signature = self.sign_message(node, data_str)
                    data = {"id": node.memory['unique_value'], "signature": signature}
                    
                    self.send_msg(node, Message(header="PreCommit", data=data, destination=list(node.neighbors())))
                    
                    for neighbor_id in node.memory['node_status'].keys():
                        self.set_alarm(node, 20, Message(header=f"Timeout_Ack_{neighbor_id}", destination=node))
                    
                    node.status = self.Status.COORDINATOR_WAITING_ACK
                else:
                    logger.info(f"Coordinator {node.memory['unique_value']} has quorum but not enough commits. Aborting.")
                    self.send_abort(node)
    
    @Status.COORDINATOR_WAITING_ACK
    def receiving(self, node: NodeAccess, message):
        # Recibo ACK de replicas, verifico firmas y cuento votos
        # Si recibo todos los ACK, envio COMMIT
        # Si no recibo todos los ACK a tiempo, aborto

        if message.header == "Ack":
            sender_id = message.source.id
            signature = message.data['signature']
            
            # Verifico firma
            data_str = f"ack:{message.data['id']}"
            if not self.verify_signature(node, data_str, signature, message.data['id']):
                logger.info(f"Coordinator {node.memory['unique_value']} rejects ACK from {message.data['id']}: invalid signature")
                return
            
            node.memory['ack_votes'][message.data['id']] = signature
            node.memory['node_status'][sender_id] = 'ack'
            
            logger.info(f"Coordinator {node.memory['unique_value']} received VALID signed ACK from {message.data['id']}")
            
            # Check if we have quorum
            if len(node.memory['ack_votes']) >= self.n - 1:
                logger.info(f"Coordinator {node.memory['unique_value']} achieved ACK QUORUM ({len(node.memory['ack_votes'])}/{self.n - 1}). Sending COMMIT.")
                
                # Firmo y envio Commit
                data_str = f"commit:{node.memory['unique_value']}"
                signature = self.sign_message(node, data_str)
                data = {"id": node.memory['unique_value'], "signature": signature}
                
                self.send_msg(node, Message(header="Commit", data=data, destination=list(node.neighbors())))
                
                for neighbor_id in node.memory['node_status'].keys():
                    self.set_alarm(node, 20, Message(header=f"Timeout_Done_{neighbor_id}", destination=node))
                
                node.status = self.Status.COORDINATOR_WAITING_DONE
    
    @Status.COORDINATOR_WAITING_ACK
    def alarm(self, node, message):
        # Si no recibo ACK de algun nodo, aborto

        if message.header.startswith("Timeout_Ack_"):
            failed_id = int(message.header.split("_")[-1])
            if node.memory['node_status'][failed_id] != 'ack':
                logger.info(f"Coordinator {node.memory['unique_value']} timeout waiting for ACK from {failed_id}. Aborting.")
                self.send_abort(node)
    
    @Status.COORDINATOR_WAITING_DONE
    def receiving(self, node, message):
        # Recibo DONE de replicas, verifico firmas y cuento votos
        # Si recibo todos los DONE, cambio a estado DONE

        if message.header == "Done":
            sender_id = message.source.id
            signature = message.data['signature']
            
            # Verifico firma
            data_str = f"done:{message.data['id']}"
            if not self.verify_signature(node, data_str, signature, message.data['id']):
                logger.info(f"Coordinator {node.memory['unique_value']} rejects DONE from {message.data['id']}: invalid signature")
                return
            
            node.memory['node_status'][sender_id] = 'done'
            logger.info(f"Coordinator {node.memory['unique_value']} received VALID signed DONE from {message.data['id']}")
            
            # Chequeo si todos los nodos honestos hicieron commit
            done_count = sum(1 for status in node.memory['node_status'].values() if status == 'done')
            if done_count >= self.n - 1:
                logger.info(f"Coordinator {node.memory['unique_value']} CONSENSUS ACHIEVED! All honest nodes committed.")
                node.status = self.Status.DONE
    
    @Status.COORDINATOR_WAITING_DONE
    def alarm(self, node, message):
        # Si no recibo DONE de algun nodo, reenvio COMMIT

        if message.header.startswith("Timeout_Done_"):
            failed_id = int(message.header.split("_")[-1])
            if node.memory['node_status'][failed_id] != 'done':
                logger.info(f"Coordinator {node.memory['unique_value']} timeout for DONE from {failed_id}. Resending COMMIT.")
                data_str = f"commit:{node.memory['unique_value']}"
                signature = self.sign_message(node, data_str)
                data = {"id": node.memory['unique_value'], "signature": signature}
                self.send_msg(node, Message(header="Commit", data=data, 
                                           destination=[n for n in node.neighbors() if n.id == failed_id]))
                self.set_alarm(node, 20, Message(header=f"Timeout_Done_{failed_id}", destination=node))
    
    @Status.COORDINATOR_ABORTING
    def receiving(self, node, message):
        # Recibo ABORTED de replicas, verifico firmas y cuento votos
        # Si recibo todos los ABORTED, cambio a estado DONE

        if message.header == "Aborted":
            sender_id = message.source.id
            node.memory['node_status'][sender_id] = 'aborted'
            
            aborted_count = sum(1 for status in node.memory['node_status'].values() if status == 'aborted')
            if aborted_count >= self.n - 1:
                logger.info(f"Coordinator {node.memory['unique_value']} received abort confirmations. Transaction aborted.")
                node.status = self.Status.DONE
    
    @Status.COORDINATOR_ABORTING
    def alarm(self, node, message):
        # Si no recibo ABORTED de algun nodo, reenvio ABORT

        if message.header.startswith("Timeout_Abort_"):
            failed_id = int(message.header.split("_")[-1])
            if node.memory['node_status'][failed_id] != 'aborted':
                logger.info(f"Coordinator {node.memory['unique_value']} resending ABORT to {failed_id}")
                data_str = f"abort:{node.memory['unique_value']}"
                signature = self.sign_message(node, data_str)
                data = {"id": node.memory['unique_value'], "signature": signature}
                self.send_msg(node, Message(header="Abort", data=data, destination=[n for n in node.neighbors() if n.id == failed_id]))
                self.set_alarm(node, 20, Message(header=f"Timeout_Abort_{failed_id}", destination=node))
    
    def send_abort(self, node):
        # Envio ABORT a todos los replicas
        data_str = f"abort:{node.memory['unique_value']}"
        signature = self.sign_message(node, data_str)
        data = {"id": node.memory['unique_value'], "signature": signature}
        self.send_msg(node, Message(header="Abort", data=data, destination=list(node.neighbors())))
        for neighbor_id in node.memory['node_status'].keys():
            self.set_alarm(node, 20, Message(header=f"Timeout_Abort_{neighbor_id}", destination=node))
        node.status = self.Status.COORDINATOR_ABORTING
    
    # ==================== REPLICA NODES ====================
    
    @Status.SLEEP
    def receiving(self, node, message):
        # Recibo PREPARE del coordinador, verifico firma y envio PREPARED

        if message.header == "Prepare":
            coordinator_id = message.data['id']
            signature = message.data['signature']
            
            # Verifico la firma
            data_str = f"prepare:{coordinator_id}"
            if not self.verify_signature(node, data_str, signature, coordinator_id):
                logger.info(f"Replica {node.memory['unique_value']} rejects PREPARE: invalid signature from {coordinator_id}")
                return
            
            logger.info(f"Replica {node.memory['unique_value']} received VALID signed PREPARE from coordinator {coordinator_id}")
            
            # Firmo y envio PREPARED response (decision=1 para commit, 0 para abortar)
            decision = 1  # Honesto vota commit
            data_str = f"prepared:{node.memory['unique_value']}:{decision}"
            signature = self.sign_message(node, data_str)
            
            data = {
                "id": node.memory['unique_value'],
                "decision": decision,
                "signature": signature
            }
            
            logger.info(f"Replica {node.memory['unique_value']} sends SIGNED PREPARED (decision={decision}, sig={signature})")
            self.send_msg(node, Message(header="Prepared", data=data, destination=message.source))
            node.status = self.Status.WAITING_PRECOMMIT
    
    @Status.WAITING_PRECOMMIT
    def receiving(self, node, message):
        # Recibo PRECOMMIT del coordinador, verifico firma y envio ACK

        if message.header == "PreCommit":
            coordinator_id = message.data['id']
            signature = message.data['signature']
            
            # Verifico la firma
            data_str = f"precommit:{coordinator_id}"
            if not self.verify_signature(node, data_str, signature, coordinator_id):
                logger.info(f"Replica {node.memory['unique_value']} rejects PRECOMMIT: invalid signature")
                return
            
            logger.info(f"Replica {node.memory['unique_value']} received VALID signed PRECOMMIT from coordinator {coordinator_id}")
            
            # Sign and send ACK
            data_str = f"ack:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            
            data = {
                "id": node.memory['unique_value'],
                "signature": signature
            }
            
            logger.info(f"Replica {node.memory['unique_value']} sends SIGNED ACK (sig={signature})")
            self.send_msg(node, Message(header="Ack", data=data, destination=message.source))
            node.status = self.Status.WAITING_COMMIT
        
        # Si recibo ABORT del coordinador, verifico firma y envio ABORTED
        elif message.header == "Abort":
            coordinator_id = message.data['id']
            data_str = f"abort:{coordinator_id}"
            signature = message.data['signature']

            if not self.verify_signature(node, data_str, signature, coordinator_id):
                logger.info(f"Replica {node.memory['unique_value']} rejects ABORT: invalid signature")
                return

            logger.info(f"Replica {node.memory['unique_value']} received ABORT. Transaction aborted.")
            data_str = f"aborted:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            data = {"id": node.memory['unique_value'], "signature": signature}
            self.send_msg(node, Message(header="Aborted", data=data, destination=message.source))
            node.status = self.Status.DONE
            
        # Si recibo PREPARE del coordinador, seguramente se perdio mensaje. Verifico firma y reenvio PREPARED 
        elif message.header == "Prepare":
            logger.info(f"Replica {node.memory['unique_value']} received duplicate PREPARE. Resending PREPARED.")
            decision = 1
            data_str = f"prepared:{node.memory['unique_value']}:{decision}"
            signature = self.sign_message(node, data_str)
            data = {"id": node.memory['unique_value'], "decision": decision, "signature": signature}
            self.send_msg(node, Message(header="Prepared", data=data, destination=message.source))
    
    @Status.WAITING_COMMIT
    def receiving(self, node, message):
        # Si recibo COMMIT del coordinador, verifico firma y envio DONE

        if message.header == "Commit":
            coordinator_id = message.data['id']
            signature = message.data['signature']
            
            # Verifico la firma
            data_str = f"commit:{coordinator_id}"
            if not self.verify_signature(node, data_str, signature, coordinator_id):
                logger.info(f"Replica {node.memory['unique_value']} rejects COMMIT: invalid signature")
                return
            
            logger.info(f"Replica {node.memory['unique_value']} received VALID signed COMMIT. COMMITTING transaction!")
            
            # Firmo y envio DONE
            data_str = f"done:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            
            data = {
                "id": node.memory['unique_value'],
                "signature": signature
            }
            
            logger.info(f"Replica {node.memory['unique_value']} sends SIGNED DONE (sig={signature})")
            self.send_msg(node, Message(header="Done", data=data, destination=message.source))
            node.status = self.Status.DONE
            
        # Si recibo ABORT del coordinador, verifico firma y envio ABORTED
        elif message.header == "Abort":
            coordinator_id = message.data['id']
            data_str = f"abort:{coordinator_id}"
            signature = message.data['signature']

            if not self.verify_signature(node, data_str, signature, coordinator_id):
                logger.info(f"Replica {node.memory['unique_value']} rejects ABORT: invalid signature")
                return

            data_str = f"aborted:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            logger.info(f"Replica {node.memory['unique_value']} received ABORT. Transaction aborted.")
            data = {"id": node.memory['unique_value'], "signature": signature}
            self.send_msg(node, Message(header="Aborted", data=data, destination=message.source))
            node.status = self.Status.DONE
            
        # Si recibo COMMIT del coordinador, seguramente se perdio mensaje. Verifico firma y reenvio DONE
        elif message.header == "PreCommit":
            logger.info(f"Replica {node.memory['unique_value']} received duplicate PRECOMMIT. Resending ACK.")
            data_str = f"ack:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            data = {"id": node.memory['unique_value'], "signature": signature}
            self.send_msg(node, Message(header="Ack", data=data, destination=message.source))
    
    # ==================== FAULTY NODES ====================
    
    @Status.FAULTY
    def receiving(self, node: NodeAccess, message):
        # El coordinador mando decision, no hay mas para hacer
        if message.header in ["Commit", "Abort", "Aborted"]:
            node.status = self.Status.DONE


        # El nodo faulty recibe un mensaje, y responde con mensajes conflictivos

        logger.info(f"FAULTY {node.memory['unique_value']} received message. Sending CONFLICTING responses!")
        
        # Elijo a quien enviar commit y a quien abort
        neighbors = list(node.neighbors())
        neighbors.remove(message.source)
        neighbors_1 = neighbors[:len(neighbors)//2]
        neighbors_2 = neighbors[len(neighbors)//2:]
        
        # Envio voto commit a algunos
        decision_commit = 1
        data_str = f"commit:{node.memory['unique_value']}:{decision_commit}"
        sig_commit = self.sign_message(node, data_str)
        self.send_msg(node, Message(header="Commit", data={"id": node.memory['unique_value'], "decision": decision_commit, "signature": sig_commit}, destination=neighbors_1))
        
        # Envio voto abort a otros (o firma invalida)
        decision_abort = 0
        data_str_abort = f"abort:{node.memory['unique_value']}:{decision_abort}"
        sig_abort = self.sign_message(node, data_str_abort)
        self.send_msg(node, Message(header="Abort", data={"id": node.memory['unique_value'], "decision": decision_commit, "signature": sig_abort}, destination=neighbors_2))


        # Envio mensaje a coordinador, haciendome pasar por otro nodo honesto
        decision_abort = 0
        data_str_abort = f"abort:3:{decision_abort}"
        sig_abort = self.sign_message(node, data_str_abort)
        self.send_msg(node, Message(header="Abort", data={"id": node.memory['unique_value'], "decision": decision_commit, "signature": sig_abort}, destination=message.source))
        
        # Envio mensaje correcto al coordinador, para asi confundirlo, que mande a hacer commit, pero a los demas les envio abort, para confundirlos y que aborten
        if message.header == "Prepare":
            decision_commit = 1
            data_str = f"prepared:{node.memory['unique_value']}:{decision_commit}"
            sig_commit = self.sign_message(node, data_str)
            data_commit = {"id": node.memory['unique_value'], "decision": decision_commit, "signature": sig_commit}
            self.send_msg(node, Message(header="Prepared", data=data_commit, destination=message.source))
        elif message.header == "PreCommit":
            data_str = f"ack:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            data = {"id": node.memory['unique_value'], "signature": signature}
            self.send_msg(node, Message(header="Ack", data=data, destination=message.source))
        elif message.header == "Commit":
            data_str = f"done:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            data = {"id": node.memory['unique_value'], "signature": signature}
            self.send_msg(node, Message(header="Done", data=data, destination=message.source))
        elif message.header == "Abort":
            data_str = f"aborted:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            data = {"id": node.memory['unique_value'], "signature": signature}
            self.send_msg(node, Message(header="Aborted", data=data, destination=message.source))
        
        logger.info(f"FAULTY {node.memory['unique_value']} sent COMMIT vote to coordinator but may send ABORT to others")

    # ==================== DONE ====================
    
    @Status.DONE
    def default(self, node, message):
        # En estado DONE, solo respondo a mensajes duplicados reenviando mi respuesta

        if message.header == "Commit":
            # Resend Done if coordinator didn't receive it
            logger.info(f"Node {node.memory['unique_value']} in DONE received duplicate COMMIT. Resending DONE.")
            data_str = f"done:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            data = {"id": node.memory['unique_value'], "signature": signature}
            self.send_msg(node, Message(header="Done", data=data, destination=message.source))
        elif message.header == "Abort":
            data_str = f"aborted:{node.memory['unique_value']}"
            signature = self.sign_message(node, data_str)
            logger.info(f"Node {node.memory['unique_value']} in DONE received ABORT. Resending Aborted.")
            data = {"id": node.memory['unique_value'], "signature": signature}
            self.send_msg(node, Message(header="Aborted", data=data, destination=message.source))
        elif not message.header.startswith("Timeout_"):
            logger.debug(f"Node {node.memory['unique_value']} in DONE ignoring {message.header}")
    
    # ========================================
    
    def sign_message(self, node: NodeAccess, message_data: str) -> str:
        # Dummy de firma digital usando hash
        # En la vida real usariamos una private key (RSA, ECDSA)
        private_key = node.memory['private_key']
        signature = hashlib.sha256(f"{message_data}:{private_key}".encode()).hexdigest()[:16]
        return signature
    
    def verify_signature(self, node: NodeAccess, message_data: str, signature: str, sender_id: int) -> bool:
        # En la vida real usariamos una public key (RSA, ECDSA)
        # Aca simulo chequeando que la firma matchea lo esperado
        expected_sig = hashlib.sha256(f"{message_data}:key_{sender_id}".encode()).hexdigest()[:16]
        is_valid = signature == expected_sig
        if not is_valid:
            logger.info(f"Node {node.memory['unique_value']} detected INVALID signature from {sender_id}")
        return is_valid
    
    def verify_key(self, key: str) -> bool:
        """Verify configuration parameters"""
        if key in ["m", "coordinatorID", "enable_faults"]:
            return True
        return False

