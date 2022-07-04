# TigerGraph PySpark Sample

A sample project reading from TigerGraph with pySpark

## Quickstart

1. Install Spark and Scala (`brew install apache-spark && brew install scala`)
1. Clone this project and enter the directory
1. Create a Python virtual environment (`python3 -m venv venv`) and enter the environment (`source venv/bin/activate`)
1. Install pySpark and pyPandoc (`pip3 install pyspark pypandoc`)
1. Load an on-premise TigerGraph AMLSim graph
1. Download the lastest `.jar` file of the JDBC TigerGraph Driver
1. Run the project (`spark-submit --jars tigergraph-jdbc-driver-1.3.0.jar index.py`)

## Overview

This repository will walk you through how to get TigerGraph data using pySpark. It shows three possible methods to do so: retrieving vertices, retrieving edges, and running queries. 

Find a thorough walkthrough of this project (set up, code explanation, etc.) [here](https://medium.com/datadriveninvestor/an-introduction-to-pyspark-and-tigergraph-9c3396835bc2).
