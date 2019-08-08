ReFlow: a reversible workflow    <img alt="ReFlow logo" src="Controller/doc/ReFlowLogo.PNG" width=50/> 
=============================

ReFlow is a reversible workflow system for building and maintaining large functional genomics databases.  It is based on a dependency graph written in XML.  It runs on UNIX only, and has been tested on Linux.  Its primary user interface is textual (command line and log files).  While it does not have a GUI, its textual tools do a good job managing very large workflows. 

ReFlow is specifically designed for use in populating and maintaining integrating databases (warehouses). Its key feature, and how it got its name, is that it is a reversible workflow.  Any step in the graph can be undone, and when it is, ReFlow runs the graph in reverse to that point, erasing from the database, or other persistent stores, any consequences of that step and its children.  After the desired corrections are made, the workflow can be run forward again.

For details, please have a look at the [ReFlow User Manual](https://docs.google.com/document/d/18pFLlkCA1inTfbOpd_NDxiMvcCu02jz29U9v2HAth84/pub)

The Source code is available under the Gnu Lesser GPL.

Please see the [ReFlow Installation Guide](https://docs.google.com/document/d/1bJr0jWktNhOf8XZtiAZDp8m0qi9Ny_pDmPMBH9vQ1mE/pub) for installation instructions.

ReFlow requires [DistribJob](http://www.google.com/url?q=https%3A%2F%2Fdocs.google.com%2Fdocument%2Fpub%3Fid%3D1BixZ5t2c0hnOZES-Rk2wG2loAQqclcmRj7AeKQjZMHA) to control jobs on compute clusters. (DistribJob sits on top of queueing systems such as SGE and PBS.)
