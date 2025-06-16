from diagrams import Cluster, Diagram, Edge
from diagrams.custom import Custom

graph_attr = {
    "bgcolor": "#0d1117",
    "fontcolor": "#c9d1d9",
}

node_attr = {
    "fontcolor": "#c9d1d9"
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
    attr = { "bgcolor": "#21262d", "fontcolor": "#c9d1d9" }
    with Cluster(f"Node Operator {n} (Private Network)", graph_attr = attr):
        attr = { "bgcolor": "#2d333b", "fontcolor": "#c9d1d9" } 
        with Cluster("WCN Nodes", graph_attr = attr):
            nodes = [PrimaryNode(), SecondaryNodes()]
        nodes >> Database()
        internet << Edge() >> nodes 

with Diagram(show=False, graph_attr = graph_attr, node_attr = node_attr, direction = "TB", filename="cluster", outformat = "svg"):
    contract = SmartContract()
    clients = Clients()
    internet = PublicInternet()

    clients << Edge() >> internet
    internet << Edge() >> contract

    attr =  { "bgcolor": "#161b22", "fontcolor": "#c9d1d9" }
    with Cluster("WCN Cluster", graph_attr = attr):
        NodeOperator("1")
        NodeOperator("...")
        NodeOperator("N")
