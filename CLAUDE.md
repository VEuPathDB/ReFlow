# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ReFlow is a reversible workflow system for building and maintaining large functional genomics databases. It's designed specifically for populating integrating databases (warehouses) with the key feature of being reversible - any step can be undone, running the graph in reverse to erase database consequences.

## Architecture

ReFlow is a multi-module Maven/Ant project with the following core components:

### Core Modules
- **GraphParser**: XML workflow parsing and graph construction (`org.gusdb.workflow.xml` package)
- **Controller**: Main workflow execution engine (`org.gusdb.workflow` package)
- **AuthoringGui**: GUI tools for workflow authoring
- **TestFlow**: Sample workflows and test cases
- **DatasetClass/DatasetLoader/StepClasses**: Supporting workflow components

### Key Classes
- `Workflow.java`: Base workflow class handling configuration, graph, and database state
- `RunnableWorkflow.java`: Executable workflow implementation
- `WorkflowStep.java`/`RunnableWorkflowStep.java`: Individual workflow step implementations
- `WorkflowXmlParser.java`: Parses XML workflow definitions into executable graphs

## Build System

ReFlow uses both Maven and Ant build systems:

### Maven Commands
```bash
mvn clean compile    # Compile all modules
mvn test            # Run tests
mvn package         # Build JAR artifacts
```

### Ant Commands
```bash
ant ReFlow-Installation                    # Install entire ReFlow system
ant ReFlow/Controller-Installation         # Install Controller module only
ant ReFlow/GraphParser-Installation        # Install GraphParser module only
```

## Development Commands

### Primary Workflow Command
The main workflow execution is through the `workflow` Perl script in `Controller/bin/`:
```bash
Controller/bin/workflow [options] <workflow.xml>
```

### Key Utilities (in Controller/bin/)
- `workflowViewer`: View workflow status and structure
- `workflowUndoMgr`: Manage workflow undo operations
- `workflowRunStep`: Execute individual workflow steps
- `workflowMakeDotFile`: Generate workflow graph visualizations
- `workflowSummary`: Generate workflow execution summaries

### Testing
Test workflows are located in `TestFlow/lib/xml/workflow/`:
- `testWorkflow.xml`: Basic workflow example
- `testSubgraph.xml`: Subgraph testing
- `testComputeCluster.xml`: Cluster execution testing

## Dependencies

- Requires Java with GUS_HOME environment variable set
- Depends on DistribJob for cluster job management
- Uses FgpUtil libraries for database and CLI utilities
- Database connectivity through configured connection pools

## Workflow Definition

Workflows are defined in XML format with dependency graphs. The system supports:
- Step dependencies and parallel execution
- Parameter passing between steps
- Database state management
- Reversible execution (undo capability)

## Environment Setup

- Set `GUS_HOME` environment variable
- Configure database connections in properties files
- Ensure DistribJob is available for cluster operations
- UNIX/Linux environment required (tested on Linux)