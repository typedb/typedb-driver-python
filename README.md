# Grakn Client for Python

[![Grabl](https://grabl.io/api/status/graknlabs/client-python/badge.svg)](https://grabl.io/graknlabs/grakn)
[![GitHub release](https://img.shields.io/github/release/graknlabs/client-python.svg)](https://github.com/graknlabs/grakn/releases/latest)
[![Discord](https://img.shields.io/discord/665254494820368395?color=7389D8&label=chat&logo=discord&logoColor=ffffff)](https://grakn.ai/discord)
[![Discussion Forum](https://img.shields.io/discourse/https/discuss.grakn.ai/topics.svg)](https://discuss.grakn.ai)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-grakn-796de3.svg)](https://stackoverflow.com/questions/tagged/grakn)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-graql-3dce8c.svg)](https://stackoverflow.com/questions/tagged/graql)

## Client Architecture
To learn about the mechanism that a Grakn Client uses to set up communication with keyspaces running on the Grakn Server, refer to [Grakn > Client API > Overview](http://dev.grakn.ai/docs/client-api/overview).

## API Reference
To learn about the methods available for executing queries and retrieving their answers using Client Python, refer to [Grakn > Client API > Python > API Reference](http://dev.grakn.ai/docs/client-api/python#api-reference).

## Concept API
To learn about the methods available on the concepts retrieved as the answers to Graql queries, refer to [Grakn > Concept API > Overview](http://dev.grakn.ai/docs/concept-api/overview)

## Install Grakn Client for Python through Pip
```
pip install grakn-client
```
If multiple Python versions are available, you may wish to use
```
pip3 install grakn-client
```

In your python program, import GraknClient:
```
from grakn.client import GraknClient
```
