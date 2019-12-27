[![PyPI version](https://badge.fury.io/py/pyddapi.svg)](https://badge.fury.io/py/pyddapi)
[![Anaconda-Server Badge](https://anaconda.org/octo/pyddapi/badges/latest_release_date.svg)](https://anaconda.org/octo/pyddapi)
[![Anaconda-Server Badge](https://anaconda.org/octo/pyddapi/badges/version.svg)](https://anaconda.org/octo/pyddapi)
[![Build Status](http://ec2-52-212-162-0.eu-west-1.compute.amazonaws.com:8080/buildStatus/icon?job=dd-api%2Fmaster)](http://ec2-52-212-162-0.eu-west-1.compute.amazonaws.com:8080/job/dd-api/job/master/)

# DDAPI introduction  
- [What is it](#what-is-it)  
- [Install](#install)  
- [Documentation](http://datadriver-doc-ddapi.s3-website-eu-west-1.amazonaws.com/)
- [Contributing](#contributing)  
  
## What is it?  
  
The following section describes the main concepts used in the Data Driver environment.  
  
### Workflow  
A Data Driver workflow is a network of tasks (python function) linked together. This workflow is typically described as a DAG (Direct Acyclic Graph). The jobs can execute all kind of Python code like data loading, feature engineering, model fitting, alerting, etc.  
  
A workflow can be scheduled and monitored in the Data Driver architecture with the tool Airflow. The Data Driver API adds data science capabilities to Airflow and the capacity to audit the input / output of each task.  
  
### Data Driver API or ddapi  
  
ddapi is a Python library. It is the access layer to Data Driver and you can use it to manipulated datasets and workflows. Some main usages are described below, for more informations and tutorials you can access to the OCTO notebook tutorials repository.  
  
```python  
import dd  
```  
  
Ddapi is composed of several modules.  
  
#### DB module  
  
    import dd.db  
  
DB is an easier way to interact with your databases. You can use it to explore your databases or import new data.   
  
#### Context module  
  
    from dd.api.contexts.distributed import AirflowContext  
    from dd.api.contexts.local import LocalContext  
  
The context is an object which will allow you to communicate with your environment during your exploration. As such, it needs to be able to communicate with your database. This is done by creating a DB object and passing it to the context constructor.  
  
#### Dataset module  
  
    import dd.api.workflow.dataset  
  
You may consider datasets as wrappers around Pandas DataFrames. It gives you access to some methods you may recognise if you are familiar with this awesome library.  
  
  
### Disclaimer  
  
#### It does not  / it is not for :  
  
 - Code versionning  
 - Enforce good code quality  
 - Data quality tool  
 - ETL  
 - Data Catalog & Data Lineage  
 - Data visualisation  
 - Datalake  
 - Magical stuffs  
 - Coffee  
  
#### It is a set of tools unified into a unique platform to accelerate data science :   
  
 - we have made an API that lets DataScientists use the same technologies they use in exploration to do industrialisation, because we saw it was the most impactfull parameter on the success of the project. (DDAPI)  
 - monitore Machine Learning models (your code + DDAPI + Airflow)  
 - schedule builds of datascience's pipeline (your code + DDAPI + Airflow)  
 - datascience feature engineering functions (your code + BDACore)  
 - metrics and datascience helpers to study model shifting (BDACore)  
 - integration of open source standards Jupyterhub, Airflow and PostgreSQL together (Lab and Factory machine roles)  
  
  
## Install  

**last release** 

    pip install pyddapi
    
    
**last build from master**     

     pip install -i https://pypi.anaconda.org/octo/label/dev/simple pyddapi

### Developer setup

#### Setup your virtual env

    virtualenv venv
    source venv/bin/activate
    pip install -e .
    pip install -r ci/tests_requirements.txt

_ddapi_ only supports python versions 2.7 and 3.6/ Running _ddapi_ with other versions is not advised, so avoid it if possible, or do it at your own risk.

You can find the package in [anaconda cloud repository](https://anaconda.org/octo/pyddapi)  
  
## Contributing  
In case you want to contribute to the code, do not forget to check our   
[Developer Guidelines](DEVGUIDE.md)

# Contributors

This repository is a part of the DataDriver project.
 
Since 2016, there were many people who contributed to this project : 

* Ali El Moussawi
* Arthur Baudry
* Augustin Grimprel
* Aurélien Massiot
* Benjamin Joyen-Conseil
* Constant Bridon
* Cyril Vinot
* Eric Biernat
* Jeffrey Lucas
* Nicolas Cavallo
* Nicolas Frot
* Matthieu Lagacherie  
* Mehdi Houacine
* Pierre Baonla Bassom
* Rémy Frenoy
* Romain Ayres
* Samuel Rochette
* Thomas Vial
* Veltin Dupont 
* Vincent Levorato
* Yannick Drant
* Yannick Schini
* Yasir Khan

