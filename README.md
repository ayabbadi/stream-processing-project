# stream-processing-project

## Customer/Product Frequency Control Solution
This repository contains a solution for managing customer and product communication frequency using streaming processing in a big data environment. The project is designed to ensure that communication, particularly via email, remains targeted, efficient, and compliant with predefined rules. It leverages Apache Flume, Spark Streaming, and other tools to process real-time data streams and apply frequency control rules.

## Key Features:
**Real-time data processing**: Utilizes Spark Streaming for processing incoming data streams in real-time.

**Frequency control**: Implements rules to manage the frequency of customer and product communications. 

**Data storage**: Stores processed data in Hadoop Distributed File System (HDFS) for further analysis and archival.

**Scalability**: Designed to handle large volumes of data generated in a big data environment.

**Configurability**: Easily adaptable to different data sources and frequency control criteria. 

This project aims to provide a solution for businesses to optimize their customer and product communications, ensuring that messages are delivered effectively while avoiding excessive or intrusive interactions.

## Lambda architecture for project
![project architecture](/schemas-img/lambda-arch.png)

## Apache flume architecture
![flume architecture](/schemas-img/flume-arch.png)

