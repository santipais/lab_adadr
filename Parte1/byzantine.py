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

class ByzantineAlgorithm(NodeAlgorithm):
    required_params=["n", "decision", "m"]

    class Status(StatusValues):
        COMMANDER = "COMMANDER"
        LIEUTIENANT = "LIEUTIENANT"
        AWAKE= "AWAKE"
        RETREAT = "RETREAT"
        ATTACK = "ATTACK"
        TRAITOR = "TRAITOR"
        DONE = "DONE"

    S_init = (Status.COMMANDER, Status.TRAITOR, Status.LIEUTIENANT)
    S_term = (Status.DONE, Status.RETREAT, Status.ATTACK, Status.TRAITOR)

    algorithm_restrictions = (
        BidirectionalLinks,
        TotalReliability,
        InitialDistinctValues,
        Connectivity,
    )

    class ByzantineMessage(Enum):
        RETREAT = 0
        ATTACK = 1

    def initializer(self):
        self.apply_restrictions()

        commander = 1
        left_traitors = self.m
        nodes = list(self.network.nodes())
        random.shuffle(nodes)

        for node in nodes:
            neighbors_source = []
            for n in range(self.n):
                neighbors_source.append({"source": n, "id": None})
            node.memory['neighbors'] = neighbors_source
            node.memory['m'] = self.m
            node.memory['saved_decisions'] = {} # Guardo las decisiones de un nodo en cada recursion. Formato { path_key: { 'decisions': {node_id: decision}, 'total': n } }
            if commander == 1:
                commander = 0
                node.status = self.Status.COMMANDER
                node.push_to_inbox(Message(meta_header=NodeAlgorithm.INI, destination=node))
                node.memory['decision'] = self.decision
            else:
                if left_traitors > 0:
                    left_traitors -= 1
                    node.status = self.Status.TRAITOR
                else:
                    node.status = self.Status.LIEUTIENANT

    @Status.COMMANDER
    def spontaneously(self, node, message):
        # Siempre honesto. Envia su decision a todos los demas nodos y termina su ejecución.

        decision = node.memory['decision']
        logger.info(f"Commander Node {node.memory['unique_value']} sends decision {self.ByzantineMessage(decision)}.")
        data = { "id": node.memory['unique_value'], "decision": decision, "m": node.memory['m'], "path": [node.memory['unique_value']], "n": self.n - 1 }
        if self.m == 0:
            data['n'] = 1

        self.send_msg(node, Message(header="Decision", data=data, destination=list(node.neighbors())))
        node.status = self.Status.DONE

    @Status.TRAITOR
    def receiving(self, node, message):
        # Se mantiene por siempre en este estado
        # Siempre que manda mensajes (cuando empieza las recursiones) miente en la mitad de los mensajes.

        neighbors_source = node.memory['neighbors']
        for neighbor in neighbors_source:
            if neighbor['source'] == message.source.id:
                neighbor['id'] = message.data['id']
                break
        node.memory['neighbors'] = neighbors_source

        if message.header == "Decision":
            match message.data['m']:
                case m if m > 1:
                    self.received_more_than_one(node, message)
                case 1:
                    self.received_one(node, message)
                case 0:
                    self.received_zero(node, message)
                case _:
                    logger.error(f"Invalid m value {message.data['m']} received by Node {node.memory['unique_value']}.")
        else:
            logger.info(f"Traitor Node {node.memory['unique_value']} in TRAITOR state received unexpected message with header {message.header}.")
    

    @Status.LIEUTIENANT
    def receiving(self, node, message):
        # Cuando termina la recursion, decide y termina en el estado ATTACK o RETREAT.

        neighbors_source = node.memory['neighbors']
        for neighbor in neighbors_source:
            if neighbor['source'] == message.source.id:
                neighbor['id'] = message.data['id']
                break
        node.memory['neighbors'] = neighbors_source

        if message.header == "Decision":
            match message.data['m']:
                case m if m > 1:
                    self.received_more_than_one(node, message)
                case 1:
                    self.received_one(node, message)
                case 0:
                    self.received_zero(node, message)
                case _:
                    logger.error(f"Invalid m value {message.data['m']} received by Node {node.memory['unique_value']}.")
        else:
            logger.info(f"Lieutenant Node {node.memory['unique_value']} in LIEUTENANT state received unexpected message with header {message.header}.")

    @Status.DONE
    def default(self, node, message):
        logger.info(f"Node {node.memory['unique_value']} is in DONE state and ignores message with header {message.header}.")
        pass
    
    def received_more_than_one(self, node: NodeAccess, message: Message):
        # Cuando recibe un mensaje con m>1, guarda la decision recibida, para el path que le llego, con su id.
        # Reenvia la decision como comandante para m-1, y sumandose al path, a los nodos que no estan en el path.


        data = message.data
        logger.info(f"{node.status} {node.memory['unique_value']} receives decision {self.ByzantineMessage(data['decision'])} with m={data['m']}. Starts recursion as commander for m={data['m']-1}, path {data['path'] + [node.memory['unique_value']]}.")
        
        key = tuple(data['path'])
        sd = node.memory['saved_decisions']
        entry = sd.get(key)
        if entry is None:
            entry = {"decisions": {node.memory['unique_value']: data['decision']}, "total": data['n']}
            sd[key] = entry
        else:
            entry['decisions'][node.memory['unique_value']] = data['decision']

        forward_data = { "id": node.memory['unique_value'], "decision": data['decision'], "m": data['m'] - 1, "path": data['path'] + [node.memory['unique_value']], "n": data['n'] - 1 }

        send_ids = [n['source'] for n in node.memory['neighbors'] if n['id'] not in data['path']]
        send_nodes = [n for n in node.neighbors() if n.id in send_ids]

        self.send_recursion_start(node, forward_data, send_nodes)

        if self.has_all_decisions(entry["decisions"], entry["total"]):
            self.process_final_decision(node, data['path'], entry["decisions"])

    def received_one(self, node: NodeAccess, message: Message):
        # Cuando recibe un mensaje con m=1, guarda la decision recibida, para el path que le llego, con su id.
        # Reenvia la decision como comandante para m-1, sin sumarse al path, a los nodos que no estan en el path.

        data = message.data
        logger.info(f"{node.status} {node.memory['unique_value']} recevies decision {self.ByzantineMessage(data['decision'])} with m={data['m']}, path {data['path']}. Sends decision as commander for m={data['m']-1}.")

        # Save received decision
        key = tuple(data['path'])
        sd = node.memory['saved_decisions']
        entry = sd.get(key)
        if entry is None:
            entry = {"decisions": {node.memory['unique_value']: data['decision']}, "total": data['n']}
            sd[key] = entry
        else:
            entry['decisions'][node.memory['unique_value']] = data['decision']

        forward_data = { "id": node.memory['unique_value'], "decision": data['decision'], "m": data['m'] - 1, "path": data['path'], "n": data['n'] }

        send_ids = [n['source'] for n in node.memory['neighbors'] if n['id'] not in data['path']]
        send_nodes = [n for n in node.neighbors() if n.id in send_ids]
        self.send_recursion_start(node, forward_data, send_nodes)
        

        if self.has_all_decisions(entry["decisions"], entry["total"]):
            self.process_final_decision(node, data['path'], entry["decisions"])

    
    def send_recursion_start(self, node: NodeAccess, data, send_nodes):
        # Cuando un nodo empieza una recursion como comandante, envia los mensajes a los nodos que no estan en el path.
        # Si es traidor, miente en la mitad de los mensajes.

        if self.Status.TRAITOR == node.status:
            half = len(send_nodes) // 2
            first_half = send_nodes[:half]
            second_half = send_nodes[half:]
            lied_decision = 1 if self.decision == 0 else 0
            logger.info(f"Traitor Lieutenant {node.memory['unique_value']} lies and sends {self.ByzantineMessage(data['decision'])} to first half and {self.ByzantineMessage(lied_decision)} to second half.")
            self.send_msg(node, Message(header="Decision", data=data, destination=first_half))
            lied_data = data.copy()
            lied_data['decision'] = lied_decision
            self.send_msg(node, Message(header="Decision", data=lied_data, destination=second_half))
        else:
            self.send_msg(node, Message(header="Decision", data=data, destination=send_nodes))

    
    def received_zero(self, node: NodeAccess, message: Message):
        # Cuando recibe un mensaje con m=0, guarda la decision recibida, para el path que le llego, con el id de quien le llego.

        data = message.data
        logger.info(f"{node.status} {node.memory['unique_value']} receives decision {self.ByzantineMessage(data['decision'])} from lieutenant {data['id']} with m={data['m']} for path {data['path']}. Saves decision.")

        key = tuple(data['path'])
        sd = node.memory['saved_decisions']
        entry = sd.get(key)
        if entry is None:
            entry = {"decisions": {data['id']: data['decision']}, "total": data['n']}
            sd[key] = entry
        else:
            entry['decisions'][data['id']] = data['decision']

        # After receiving all messages for m=0, decide based on majority
        if self.has_all_decisions(entry["decisions"], entry["total"]):
            self.process_final_decision(node, data['path'], entry["decisions"])
    

    def has_all_decisions(self, decisions, total) -> bool:
        # Calculo si tengo todas las decisiones recibidas para una recursión en especifico.

        if len(decisions) > total:
            raise ValueError("Received more decisions than expected.")
        return len(decisions) == total
    
    def majority_decision(self, decisions) -> int:
        # Calculo la decision mayoritaria entre las decisiones recibidas.

        attack_count = list(decisions.values()).count(1)
        retreat_count = list(decisions.values()).count(0)
        return 1 if attack_count > retreat_count else 0

    def process_final_decision(self, node: NodeAccess, path, decisions):
        # Cuando tengo todas las decisiones recibidas para una recursión en especifico, calculo la decision mayoritaria.

        final_decision = self.majority_decision(decisions)
        saved_decisions = node.memory['saved_decisions']
        father = path[-1]

        # Si tengo todas las decisiones y estoy en el nivel mas alto (en el path esta solo el comandante)
        # Significa que termino la ejecuión de mi algoritmo.
        if len(path) == 1:
            if self.Status.TRAITOR == node.status:
                logger.info(f"Traitor Lieutenant {node.memory['unique_value']} ends his algorithm.")
                return
            logger.info(f"{node.status} {node.memory['unique_value']} makes final decision {self.ByzantineMessage(final_decision)} for path {path} based on received decisions: {decisions}. This is the top-level lieutenant, so the algorithm ends here.")
            for entry in saved_decisions.values():
                entry['decisions'] = dict(sorted(entry['decisions'].items()))

            node.status = self.Status.ATTACK if final_decision == 1 else self.Status.RETREAT
            return

        # Si no estoy en el nivel mas alto, significa que recibi todos los mensajes para esta recursión.
        # Entonces, tomo al útlimo nodo del path como mi "padre" y guardo, en (path - padre), su decision con el id de mi padre.
        # Ya que esto significa que se termino la ejecución de la recursión que me hizo llamar este padre.

        logger.info(f"{node.status} {node.memory['unique_value']} makes final decision {self.ByzantineMessage(final_decision)} for path {path} based on received decisions: {decisions}.")

        path = tuple(path[:-1])
        entry = saved_decisions[path]
        entry['decisions'][father] = final_decision

        if self.has_all_decisions(entry["decisions"], entry["total"]):
            self.process_final_decision(node, path, entry["decisions"])
        


