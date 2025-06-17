from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom

def attr(bgcolor = None):
    return {
        "bgcolor": bgcolor,
        "fontcolor": "#c9d1d9",
        "fontname": "DejaVu Sans"
    }

def Clients():
    return Custom("Clients", "./icons/users.svg")

def PublicInternet():
    return Custom("Public Internet", "./icons/cloud.svg")

def PrimaryNode():
    return Custom("Primary", "./icons/server.svg")

def SecondaryNodes():
    return Custom("Secondary (1..M)", "./icons/server-stack.svg")

def Database():
    return Custom("WCN DB", "./icons/db.svg")

def SmartContract():
    return Custom("Smart-Contract", "./icons/document-text.svg")

def NodeOperator(n):
    with Cluster(f"Node Operator {n} (Private Network)", graph_attr = attr("#21262d")):
        with Cluster("WCN Nodes", graph_attr = attr("#2d333b")):
            nodes = [PrimaryNode(), SecondaryNodes()]
        nodes >> Database()
        internet << Edge() >> nodes 

with Diagram(show=False, graph_attr = attr("#0d1117"), node_attr = attr(), direction = "TB", filename="cluster", outformat = "svg"):
    contract = SmartContract()
    clients = Clients()
    internet = PublicInternet()

    clients << Edge() >> internet
    internet << Edge() >> contract

    with Cluster("WCN Cluster", graph_attr = attr("#161b22")):
        NodeOperator("1")
        NodeOperator("...")
        NodeOperator("N")
