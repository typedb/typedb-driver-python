PyPI package: https://pypi.org/project/typedb-client
Documentation: https://docs.vaticle.com/docs/client-api/python

## Distribution

Available through https://pypi.org

```
pip install typedb-client=={version}
```


## New Features
- **Protocol versioning**
  
  We use a new protocol API to perform a "connection open". This API does server-side protocol version compatibility checks, and replaces also our previous need to an any RPC call to check that the network is available.
  
  This API is called transparently during the construction of both the Core and Cluster clients, so the user does not have to do anything.
  
  
- **Introduce Values to support expressions**
  
  Introduce the 'Value' type, which is returned as the result of an expression's computation. This change follows from https://github.com/vaticle/typeql/pull/260, which outlines the capabilities of the new expression syntax.
  
  Values (representing any of Long, Double, Boolean, String, or DateTime) are returned as part of `ConceptMap` answers and are subtypes of `Concept` for the time being. Their main API is made of the `.get_value()` method and `.get_value_vype()` method, along with all the standard safe downcasting methods to convert a `Concept` into a `Value`, using `Concept.is_value()` and `Concept.as_value()`.
  
  We also move the import location of `ValueType` from `attribute_type.py` to `concept.py`. We remove APIs like `isKeyable` from `ValueType`s.
  
  
- **Support generalised annotations and uniqueness**
  
  We update the Typedb Protocol and TypeQL to the latest versions, which support the uniqueness annotation and generalised `Type` APIs. These generalised APIs allow querying by a set of annotations, rather than just boolean = `true|false`.
  
  For example, the API: `AttributeType.get_owners(onlyKey: boolean)`, has become: `AttributeType.get_owners(annotations: Set["Annotation"])`
  
  All usages of boolean flags to indicate key-ness should be replaced by a set of `ThingType.Annotations.KEY`. The new `@unique` annotation is available as `ThingType.Annotations.KEY`, and also usable within the APIs that accept annotations.
  
  
  
- **Add method to Cluster client to retrieve current user**
  
  Add an API to be able to retrieve the currently authenticated user.
  
  

## Bugs Fixed
- **Fix Factory Badge**
  
  We've updated the Factory badge from the old 'Grabl' badge that is no longer active.
  
  

## Code Refactors


## Other Improvements
- **Update release notes workflow**
  
  We integrate the new release notes tooling. The release notes are now to be written by a person and committed to the repo.
  
  
- **Split connection test jobs**



