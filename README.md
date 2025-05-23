# Aerospike Data Explorer

A Java-based GUI application for interacting with Aerospike databases. This tool allows users to connect to an Aerospike server, execute AQL queries, and view the results in a table or JSON format. The application supports a wide range of AQL queries, including SELECT, INSERT, DELETE, UPDATE, and more advanced features like filtering, sorting, and aggregations.

## Table of Contents

- [Features](#features)
- [Screenshots](#screenshots)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Running the Application](#running-the-application)
- [Usage](#usage)
  - [Connecting to Aerospike](#connecting-to-aerospike)
  - [Executing AQL Queries](#executing-aql-queries)
  - [Exporting Data](#exporting-data)
  - [Deleting Records and Sets](#deleting-records-and-sets)
- [Advanced Features](#advanced-features)
  - [Filtering and Sorting](#filtering-and-sorting)
  - [Aggregations](#aggregations)
  - [Batch Operations](#batch-operations)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Features

- **Connect to Aerospike**: Establish a connection to an Aerospike server using host information and optional user credentials.
- **Execute AQL Queries**: Run a wide range of AQL queries (SELECT, INSERT, DELETE, UPDATE) and view the results.
- **Display Results**: View query results in a table or JSON format.
- **Export Data**: Export query results to JSON or CSV files.
- **Delete Records and Sets**: Delete specific records or entire sets from the database.
- **User-Friendly Interface**: Intuitive graphical interface for easy interaction.
- **Advanced Features**: Support for filtering, sorting, aggregations, and batch operations.

## Screenshots

![Main Interface](https://example.com/path/to/main_interface.png)
*The main interface of the Aerospike Data Explorer.*

![AQL Help](https://example.com/path/to/aql_help.png)
*The AQL help dialog providing syntax and usage examples.*

## Getting Started

### Prerequisites

- **Java Development Kit (JDK)**: JDK 11 or higher is required.
- **Maven**: For dependency management.
- **Aerospike Server**: An Aerospike server running and accessible.

### Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/aerospike-data-explorer.git
